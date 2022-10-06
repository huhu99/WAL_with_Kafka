package recorder

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

const (
	resultPath = "data/results/result.csv"
)

type Recorder struct {
	init bool
	file *os.File
}

func (r *Recorder) WriteResult(throughputs []uint64, config *sarama.Config, isAsync bool) error {
	csvFile, err := os.OpenFile(resultPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return fmt.Errorf("[WriteResult]: failed creating file: %s", err)
	}
	csvwriter := csv.NewWriter(csvFile)
	// mark that the file has been created
	if !r.init {
		r.init = true
		r.file = csvFile
		// write header only once
		err = csvwriter.Write([]string{"Async", "ACK", "Idempotent", "MaxOpenReqs", "FlushThreshold", "Throughput(MBs/sec)"})
		if err != nil {
			return fmt.Errorf("[WriteResult]: failed writing header to file: %s", err)
		}
	}
	// write data
	for i := 0; i < len(throughputs); i++ {
		err = csvwriter.Write([]string{
			fmt.Sprintf("%t", isAsync),
			fmt.Sprintf("%d", config.Producer.RequiredAcks),
			fmt.Sprintf("%t", config.Producer.Idempotent),
			fmt.Sprintf("%d", config.Net.MaxOpenRequests),
			fmt.Sprintf("%d", config.Producer.Flush.Messages),
			fmt.Sprintf("%d", throughputs[i])})
		if err != nil {
			return fmt.Errorf("[WriteResult]: failed writing data to file: %s", err)
		}
	}
	csvwriter.Flush()
	return nil
}

func (r *Recorder) Close() error {
	return r.file.Close()
}
