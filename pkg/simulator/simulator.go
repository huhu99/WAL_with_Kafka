package simulator

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tidwall/wal"
)

const (
	topic          = "WAL-benchmark"
	brokerAddress1 = "localhost:9093"
	brokerAddress2 = "localhost:9094"
	brokerAddress3 = "localhost:9095"
	MBint          = 1024 * 1024
)

type Counter struct {
	keepCounting bool
}

// GenerateWALLogs
// This function utilizes the WAL package to generate 50 WAL log files
// The sizes of these of WALs range from 20 to 200 records
// I deliberately made every record a 5 byte array, so the size of these WALs
// should range from 100 bytes to 1KB
///*
func GenerateWALLogs(path string) error {
	isEmpty, err := IsEmpty(path)
	if err != nil {
		return err
	}
	if !isEmpty {
		fmt.Println("[GenerateWALLogs]: directory not empty, WALs already exist")
		return nil
	}
	rand1 := rand.New(rand.NewSource(20220414))
	var size int
	for i := 0; i < 50; i++ {
		walLog, err1 := wal.Open(path+"/log"+strconv.Itoa(i), nil)
		if err1 != nil {
			fmt.Println(err1)
			return err1
		}
		size = rand1.Intn(181) + 20 // [0,181)+20=[20,200]
		for j := 0; j < size; j++ {
			walLog.Write(uint64(j), []byte("entry"))
		}
		walLog.Close()
	}
	fmt.Println("successfully generated raw WAL logs")

	err2 := ExtractAllLogs(path)
	if err2 != nil {
		fmt.Println("failed to extract files from raw WAL logs")
		return err2
	}
	fmt.Println("[GenerateWALLogs]: successfully extracted all WAL logs")
	return nil
}

// SyncProducerBenchmark
// This benchmark is achieved with Sarama library's syncproducer
// Relatively faster compared to both producers provided in kafka-go
// Able to change values of requiredACKs and results indicate significant difference between different ACKs
// Function outputs the throughput to stdout in the form of MB/s/*
func SyncProducerBenchmark(path string, seconds int, kafkaConfig *sarama.Config) (uint64, error) {
	allFiles, err := ReadAllFiles(path)
	rand1 := rand.New(rand.NewSource(20220415))
	size := len(allFiles)
	if err != nil {
		return 0, err
	}
	// set up a sync producer
	conn, err := sarama.NewSyncProducer([]string{brokerAddress1, brokerAddress2, brokerAddress3}, kafkaConfig)
	if err != nil {
		return 0, err
	}
	intChannel := make(chan uint64)
	counter := &Counter{keepCounting: true}
	go func() {
		i := uint64(0)
		for counter.keepCounting {
			index := rand1.Intn(size) //[0,size)
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(allFiles[index]),
			}
			_, _, err1 := conn.SendMessage(msg)
			if err1 != nil {
				log.Fatal("failed to write messages:", err)
			}
			i += uint64(len(allFiles[index]))
		}
		intChannel <- i
		if err := conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()
	t1 := time.NewTimer(time.Duration(seconds) * time.Second)
	<-t1.C
	counter.keepCounting = false
	fmt.Printf("Timer duration is %vs\n", seconds)
	numOfMessages := <-intChannel
	throughputPerSec := numOfMessages / uint64(seconds*MBint)
	fmt.Printf("Throughput is %v MB/s \n", throughputPerSec)
	return throughputPerSec, nil
}

// AsyncProducerBenchmark
// This benchmark is achieved with Sarama library's asyncproducer, which is the faster than syncproducer
func AsyncProducerBenchmark(path string, seconds int, kafkaConfig *sarama.Config) (uint64, error) {
	allFiles, err := ReadAllFiles(path)
	rand1 := rand.New(rand.NewSource(20220415))
	size := len(allFiles)
	if err != nil {
		return 0, err
	}
	// set up an async producer
	conn, err := sarama.NewAsyncProducer([]string{brokerAddress1, brokerAddress2, brokerAddress3}, kafkaConfig)
	if err != nil {
		return 0, err
	}
	intChannel := make(chan uint64)
	counter := &Counter{keepCounting: true}
	go func() {
		i := uint64(0)
		for counter.keepCounting {
			select {
			case err1, ok := <-conn.Errors():
				if ok {
					log.Fatal("failed to write messages:", err1)
				} else {
					log.Fatal("channel closed")
				}
			default:
				//do nothing
			}
			index := rand1.Intn(size) //[0,size)
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(allFiles[index]),
			}
			conn.Input() <- msg
			i += uint64(len(allFiles[index]))
		}
		// send the total size of the messages to the channel
		intChannel <- i
		conn.AsyncClose()
	}()
	t1 := time.NewTimer(time.Duration(seconds) * time.Second)
	// expire the timer
	<-t1.C
	counter.keepCounting = false
	fmt.Printf("Timer duration is %vs\n", seconds)
	numOfMessages := <-intChannel
	throughputPerSec := numOfMessages / uint64(seconds*MBint)
	fmt.Printf("Throughput is %v MB/s \n", throughputPerSec)
	return throughputPerSec, nil
}
