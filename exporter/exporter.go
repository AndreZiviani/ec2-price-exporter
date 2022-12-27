package exporter

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// Exporter implements the prometheus.Exporter interface, and exports AWS Spot Price metrics.
type Exporter struct {
	productDescriptions []string
	operatingSystems    []string
	regions             []string
	lifecycle           []string
	duration            prometheus.Gauge
	scrapeErrors        prometheus.Gauge
	totalScrapes        prometheus.Counter
	pricingMetrics      map[string]*prometheus.GaugeVec
	instances           map[string]Instance
	awsCfg              aws.Config
	cache               int
	nextScrape          time.Time
	errorCount          uint64
	metricsMtx          sync.RWMutex
	sync.RWMutex
}

type scrapeResult struct {
	Name               string
	Value              float64
	Region             string
	AvailabilityZone   string
	InstanceType       string
	InstanceLifecycle  string
	ProductDescription string
	OperatingSystem    string
	Memory             string
	VCpu               string
}

// NewExporter returns a new exporter of AWS EC2 Price metrics.
func NewExporter(pds []string, oss []string, regions []string, lifecycle []string, cache int) (*Exporter, error) {

	e := Exporter{
		productDescriptions: pds,
		operatingSystems:    oss,
		regions:             regions,
		lifecycle:           lifecycle,
		cache:               cache,
		nextScrape:          time.Now(),
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "aws_pricing",
			Name:      "scrape_duration_seconds",
			Help:      "The scrape duration.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "aws_pricing",
			Name:      "scrapes_total",
			Help:      "Total AWS autoscaling group scrapes.",
		}),
		scrapeErrors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "aws_pricing",
			Name:      "scrape_error",
			Help:      "The scrape error status.",
		}),
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		log.WithError(err).Errorf("error while initializing aws config [region=%s]", "us-east-1")
	}

	e.awsCfg = cfg

	e.initGauges()
	e.getInstances()
	return &e, nil
}

func (e *Exporter) initGauges() {
	e.pricingMetrics = map[string]*prometheus.GaugeVec{}
	e.pricingMetrics["ec2"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "aws_pricing",
		Name:      "ec2",
		Help:      "Current price of the instance type.",
	}, []string{"instance_lifecycle", "instance_type", "region", "availability_zone", "product_description", "operating_system", "memory", "vcpu"})

	e.pricingMetrics["ec2_memory"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "aws_pricing",
		Name:      "ec2_memory",
		Help:      "Price of each GB of memory of the instance.",
	}, []string{"instance_lifecycle", "instance_type", "region", "availability_zone"})

	e.pricingMetrics["ec2_vcpu"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "aws_pricing",
		Name:      "ec2_vcpu",
		Help:      "Price of each VCPU of the instance.",
	}, []string{"instance_lifecycle", "instance_type", "region", "availability_zone"})
}

// Describe outputs metric descriptions.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range e.pricingMetrics {
		m.Describe(ch)
	}
	ch <- e.duration.Desc()
	ch <- e.totalScrapes.Desc()
	ch <- e.scrapeErrors.Desc()
}

// Collect fetches info from the AWS API
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {

	now := time.Now()

	if now.After(e.nextScrape) {
		pricingScrapes := make(chan scrapeResult)

		e.Lock()
		defer e.Unlock()

		e.initGauges()
		go e.scrape(pricingScrapes)
		e.setPricingMetrics(pricingScrapes)

		e.nextScrape = time.Now().Add(time.Second * time.Duration(e.cache))
	}

	e.duration.Collect(ch)
	e.totalScrapes.Collect(ch)
	e.scrapeErrors.Collect(ch)

	for _, m := range e.pricingMetrics {
		m.Collect(ch)
	}
}

func (e *Exporter) scrape(scrapes chan<- scrapeResult) {

	defer close(scrapes)
	now := time.Now()

	e.totalScrapes.Inc()

	var errorCount uint64
	log.Debugf("before for %v\n", e.regions)

	var wg sync.WaitGroup
	for _, region := range e.regions {
		if !e.inRegions(region) {
			log.Debugf("Skipping region %s", region)
			continue
		}

		log.Debugf("querying ec2 prices [region=%s]", region)
		wg.Add(1)
		go func(region string) {
			defer wg.Done()

			cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
			if err != nil {
				log.WithError(err).Errorf("error while initializing aws config [region=%s]", region)
				atomic.AddUint64(&errorCount, 1)
			}

			e.awsCfg = cfg

			if contains(e.lifecycle, "spot") {
				e.getSpotPricing(region, scrapes)
			}

			if contains(e.lifecycle, "ondemand") {
				e.getOnDemandPricing(region, scrapes)
			}
			return

		}(region)
		wg.Wait()
	}

	e.scrapeErrors.Set(float64(atomic.LoadUint64(&errorCount)))
	e.duration.Set(float64(time.Now().UnixNano()-now.UnixNano()) / 1_000_000_000)
}

func (e *Exporter) setPricingMetrics(scrapes <-chan scrapeResult) {
	log.Debug("set pricing metrics")
	for scr := range scrapes {
		name := scr.Name
		if _, ok := e.pricingMetrics[name]; !ok {
			e.metricsMtx.Lock()
			//defer e.metricsMtx.Unlock()
			if name == "ec2" {
				e.pricingMetrics[name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Namespace: "aws_pricing",
					Name:      name,
				}, []string{"instance_lifecycle", "instance_type", "region", "availability_zone", "product_description", "operating_system", "memory", "vcpu"})
			} else if name == "ec2_memory" || name == "ec2_vcpu" {
				e.pricingMetrics[name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Namespace: "aws_pricing",
					Name:      name,
				}, []string{"instance_lifecycle", "instance_type", "region", "availability_zone"})
			}
			e.metricsMtx.Unlock()
		}
		var labels prometheus.Labels
		if name == "ec2" {
			labels = map[string]string{
				"instance_lifecycle":  scr.InstanceLifecycle,
				"instance_type":       scr.InstanceType,
				"region":              scr.Region,
				"availability_zone":   scr.AvailabilityZone,
				"product_description": scr.ProductDescription,
				"operating_system":    scr.OperatingSystem,
				"memory":              scr.Memory,
				"vcpu":                scr.VCpu,
			}
		} else if name == "ec2_memory" || name == "ec2_vcpu" {
			labels = map[string]string{
				"instance_lifecycle": scr.InstanceLifecycle,
				"instance_type":      scr.InstanceType,
				"region":             scr.Region,
				"availability_zone":  scr.AvailabilityZone,
			}
		}
		e.pricingMetrics[name].With(labels).Set(float64(scr.Value))
	}
}

func (e *Exporter) inRegions(r string) bool {
	for _, region := range e.regions {
		if r == region {
			return true
		}
	}
	return false
}

func contains(elems []string, v string) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}
	return false
}
