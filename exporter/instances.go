package exporter

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	log "github.com/sirupsen/logrus"
)

const (
	// AWS doesn’t share the relationship between CPU and memory for each instance type, therefore we get this info from GCP.
	// Obviously, it could be some differences between the cpu/memory relationship between the cloud providers but using the GCP
	// relationship could give us a fairly approximate global idea and allow us know the cost of our pods and namespaces.

	// To simplify operations and taking into account an approximate global idea would be accepted the CPU-Memory relationship is
	// calculated as:

	// CPU-cost = 7.2 memory-GB-cost

	// https://engineering.empathy.co/cloud-finops-part-4-kubernetes-cost-report/
	cpuMemRelation = 7.2
)

func (e *Exporter) getInstances() {
	e.instances = make(map[string]Instance)
	ec2Svc := ec2.NewFromConfig(e.awsCfg)
	pag := ec2.NewDescribeInstanceTypesPaginator(
		ec2Svc,
		&ec2.DescribeInstanceTypesInput{})
	for pag.HasMorePages() {
		instances, err := pag.NextPage(context.TODO())
		if err != nil {
			log.WithError(err).Errorf("error while fetching available instance types")
			atomic.AddUint64(&e.errorCount, 1)
			break
		}
		for _, instance := range instances.InstanceTypes {
			e.instances[string(instance.InstanceType)] = Instance{
				Memory: aws.ToInt64(instance.MemoryInfo.SizeInMiB),
				VCpu:   aws.ToInt32(instance.VCpuInfo.DefaultVCpus),
			}
		}
	}
}

func (e Exporter) getInstanceMemory(instance string) string {
	return strconv.Itoa(int(e.instances[instance].Memory))
}

func (e Exporter) getInstanceVCpu(instance string) string {
	return strconv.Itoa(int(e.instances[instance].VCpu))
}

func (e Exporter) getNormalizedCost(value float64, instance string) (float64, float64) {
	vcpu := e.instances[instance].VCpu
	memory := e.instances[instance].Memory / 1024

	memoryCost := value / (cpuMemRelation*float64(vcpu) + float64(memory))
	vcpuCost := cpuMemRelation * memoryCost

	return vcpuCost, memoryCost
}
