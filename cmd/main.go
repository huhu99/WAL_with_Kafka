package main

import (
	"WALBenchmark/pkg/recorder"
	"WALBenchmark/pkg/simulator"
	"fmt"

	"github.com/Shopify/sarama"
)

const (
	rootPath       = "data/Write_Ahead_Logs"
	intervalInSecs = 5
)

// use options to configure kafka settings. This function returns two slices of configurations
// one for async producer and one for sync producer.
func getKafkaConfigs() ([]*sarama.Config, []*sarama.Config) {
	return []*sarama.Config{
			newKafkaConfig(WithIdempotenceEnabled(), WithMaxReqNum(1), WithFlushThreshold(1)),
			newKafkaConfig(WithWaitForLocal(), WithMaxReqNum(5), WithFlushThreshold(1)),
		}, []*sarama.Config{
			newKafkaConfig(WithWaitForLocal(), WithFlushThreshold(1), WithReturnSuccess(true)),
			newKafkaConfig(WithNoResponse(), WithFlushThreshold(1), WithReturnSuccess(true)),
			newKafkaConfig(WithWaitForAll(), WithFlushThreshold(1), WithReturnSuccess(true)),
		}
}

func main() {
	err := simulator.GenerateWALLogs(rootPath)
	if err != nil {
		fmt.Println("failed to generate WAL logs: ", err)
	}
	asyncKafkaConfigs, syncKafkaConfigs := getKafkaConfigs()
	recorder := &recorder.Recorder{}

	// async producer benchmark
	for i := 0; i < len(asyncKafkaConfigs); i++ {
		throughputs := []uint64{}
		for j := 0; j < 5; j++ {
			throughputPerSec, err := simulator.AsyncProducerBenchmark(rootPath, intervalInSecs, asyncKafkaConfigs[i])
			if err != nil {
				fmt.Println("failed to run benchmark: ", err)
			}
			throughputs = append(throughputs, throughputPerSec)
		}
		// write results to csv file
		err = recorder.WriteResult(throughputs, asyncKafkaConfigs[i], true)
		if err != nil {
			fmt.Println(err)
		}
	}
	// sync producer benchmark
	for i := 0; i < len(syncKafkaConfigs); i++ {
		throughputs := []uint64{}
		for j := 0; j < 5; j++ {
			throughputPerSec, err := simulator.SyncProducerBenchmark(rootPath, intervalInSecs, syncKafkaConfigs[i])
			if err != nil {
				fmt.Println("failed to run benchmark: ", err)
			}
			throughputs = append(throughputs, throughputPerSec)
		}
		// write results to csv file
		err = recorder.WriteResult(throughputs, syncKafkaConfigs[i], false)
		if err != nil {
			fmt.Println(err)
		}
	}
	// close recorder
	if err := recorder.Close(); err != nil {
		fmt.Println("failed to close the recorder", err)
	}
}
