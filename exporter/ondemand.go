package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	pricingtypes "github.com/aws/aws-sdk-go-v2/service/pricing/types"
	log "github.com/sirupsen/logrus"
)

const (
	TermOnDemand string = "JRTCKXETXF"
	TermPerHour  string = "6YS6EN2CT7"
)

func (e *Exporter) getOnDemandPricing(region string, scrapes chan<- scrapeResult) {
	tmpCfg := e.awsCfg
	tmpCfg.Region = "us-east-1" // this service is only available in us-east-1
	pricingSvc := pricing.NewFromConfig(tmpCfg)

	azs := e.getAZs(region)
	pricelists := make([]pricing.GetProductsOutput, 0)
	for _, os := range e.operatingSystems {
		pag := pricing.NewGetProductsPaginator(
			pricingSvc,
			&pricing.GetProductsInput{
				ServiceCode: aws.String("AmazonEC2"),
				MaxResults:  aws.Int32(100),
				Filters: []pricingtypes.Filter{
					{
						Field: aws.String("regionCode"),
						Type:  pricingtypes.FilterTypeTermMatch,
						Value: aws.String(region),
					},
					{
						Field: aws.String("capacitystatus"),
						Type:  pricingtypes.FilterTypeTermMatch,
						Value: aws.String("Used"),
					},
					{
						Field: aws.String("tenancy"),
						Type:  pricingtypes.FilterTypeTermMatch,
						Value: aws.String("Shared"),
					},
					{
						Field: aws.String("preInstalledSw"),
						Type:  pricingtypes.FilterTypeTermMatch,
						Value: aws.String("NA"),
					},
					{
						Field: aws.String("operatingSystem"),
						Type:  pricingtypes.FilterTypeTermMatch,
						Value: aws.String(os),
					},
				},
			},
		)
		for pag.HasMorePages() {
			pricelist, err := pag.NextPage(context.TODO())

			if err != nil {
				log.WithError(err).Errorf("error while fetching spot price history [region=%s]", region)
				atomic.AddUint64(&e.errorCount, 1)
			}

			pricelists = append(pricelists, *pricelist)
		}
	}

	outs := make([]Pricing, 0)
	for _, pricelist := range pricelists {
		for _, price := range pricelist.PriceList {
			var tmp Pricing
			log.Debug(price)
			json.Unmarshal([]byte(price), &tmp)
			outs = append(outs, tmp)
		}
	}

	for _, out := range outs {
		sku := out.Product.Sku
		skuOnDemand := fmt.Sprintf("%s.%s", sku, TermOnDemand)
		skuOnDemandPerHour := fmt.Sprintf("%s.%s", skuOnDemand, TermPerHour)

		value, err := strconv.ParseFloat(out.Terms.OnDemand[skuOnDemand].PriceDimensions[skuOnDemandPerHour].PricePerUnit["USD"], 64)
		if err != nil {
			log.WithError(err).Errorf("error while parsing spot price value from API response [region=%s, type=%s]", region, out.Product.Attributes["instanceType"])
			atomic.AddUint64(&e.errorCount, 1)
		}
		log.Debugf("Creating new metric: ec2{region=%s, instance_type=%s, product_description=%s} = %v.", region, out.Product.Attributes["instanceType"], out.Product.Attributes["operatingSystem"], value)

		vcpu, memory := e.getNormalizedCost(value, out.Product.Attributes["instanceType"])
		for _, az := range azs {
			scrapes <- scrapeResult{
				Name:               "ec2",
				Value:              value,
				Region:             region,
				AvailabilityZone:   az,
				InstanceType:       out.Product.Attributes["instanceType"],
				InstanceLifecycle:  "ondemand",
				OperatingSystem:    out.Product.Attributes["operatingSystem"],
				ProductDescription: out.Product.Attributes["productDescription"],
				Memory:             e.getInstanceMemory(out.Product.Attributes["instanceType"]),
				VCpu:               e.getInstanceVCpu(out.Product.Attributes["instanceType"]),
			}
			scrapes <- scrapeResult{
				Name:              "ec2_memory",
				Value:             memory,
				Region:            region,
				AvailabilityZone:  az,
				InstanceType:      out.Product.Attributes["instanceType"],
				InstanceLifecycle: "ondemand",
			}
			scrapes <- scrapeResult{
				Name:              "ec2_vcpu",
				Value:             vcpu,
				Region:            region,
				AvailabilityZone:  az,
				InstanceType:      out.Product.Attributes["instanceType"],
				InstanceLifecycle: "ondemand",
			}
		}
	}

}

func (e *Exporter) getAZs(region string) []string {
	ec2Svc := ec2.NewFromConfig(e.awsCfg)

	tmpazs, err := ec2Svc.DescribeAvailabilityZones(context.TODO(), &ec2.DescribeAvailabilityZonesInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("group-name"),
				Values: []string{region},
			},
		}})

	if err != nil {
		log.WithError(err).Fatalf("Couldn't describe AZs in %s", region)
	}

	azs := make([]string, len(tmpazs.AvailabilityZones))
	for i, az := range tmpazs.AvailabilityZones {
		azs[i] = *az.ZoneName
	}

	return azs
}
