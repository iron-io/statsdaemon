package main

import (
  "bytes"
  "fmt"
  "strconv"
  "math"
    "sort"
	"encoding/json"
	"regexp"

	"gopkg.in/inconshreveable/log15.v2"
)

type Metric struct {
	Regexp            string         `json:"regexp"`
	RegexpCompiled    *regexp.Regexp `json:"-"`
	Threshold         float64        `json:"threshold"`
	PercentThresholds []float64      `json:"percent-thresholds"`
	CountPersistence  bool           `json:"count_persistence"`
	Functions         []string       `json:"func"`
}

var (
  timersFlags     = make(map[string]bool)
  timersMetrics   = make(map[string]Metric)
)

type configuration struct {
	Metrics []Metric `json:"metrics"`
}
type processor struct {
  config *configuration
}
func (*processor) processCounters(buffer *bytes.Buffer, now int64) int64 {
	var num int64
	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range counters {
		fmt.Fprintf(buffer, "%s %s %d\n", bucket, strconv.FormatFloat(value, 'f', -1, 64), now)
		delete(counters, bucket)
		countInactivity[bucket] = 0
		num++
	}
	for bucket, purgeCount := range countInactivity {
		if purgeCount > 0 {
			fmt.Fprintf(buffer, "%s 0 %d\n", bucket, now)
			num++
		}
		countInactivity[bucket] += 1
		if countInactivity[bucket] > *persistCountKeys {
			delete(countInactivity, bucket)
		}
	}
	return num
}

func (*processor) processGauges(buffer *bytes.Buffer, now int64) int64 {
	var num int64

	for bucket, currentValue := range gauges {
		fmt.Fprintf(buffer, "%s %s %d\n", bucket, strconv.FormatFloat(currentValue, 'f', -1, 64), now)
		num++
		if *deleteGauges {
			delete(gauges, bucket)
		}
	}
	return num
}

func (*processor) processSets(buffer *bytes.Buffer, now int64) int64 {
	num := int64(len(sets))
	for bucket, set := range sets {

		uniqueSet := map[string]bool{}
		for _, str := range set {
			uniqueSet[str] = true
		}

		fmt.Fprintf(buffer, "%s %d %d\n", bucket, len(uniqueSet), now)
		delete(sets, bucket)
	}
	return num
}

func (*processor) processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for bucket, timer := range timers {
		bucketWithoutPostfix := bucket[:len(bucket)-len(*postfix)]
		num++
		if _, ok := timersFlags[bucketWithoutPostfix]; !ok {
			log15.Debug("No metrics are configured for this bucket. Maybe you need block with regexp '.*' in the end of config. Ignoring calculations & continuing.", "bucket", bucketWithoutPostfix)
			continue
		}

		var percentiles Percentiles
		metric, metricExists := timersMetrics[bucketWithoutPostfix]
		if metricExists {
			percent := Percentiles{}
			for _, percThr := range metric.PercentThresholds {
				percent = append(percent, &Percentile{percThr, strconv.Itoa(int(percThr))})
			}
			percentiles = percent
		}

		sort.Sort(timer)
		min := timer[0]
		max := timer[len(timer)-1]
		maxAtThreshold := max
		count := len(timer)

		var violationsCount int
		sum := float64(0)
		for _, value := range timer {
			sum += value
			if metricExists && value > metric.Threshold {
				violationsCount++
			}
		}
		mean := sum / float64(len(timer))

		sumF := float64(0)
		for _, value := range timer {
			sumF += math.Pow(value-mean, 2.0)
		}
		stDeviation := math.Sqrt(sumF / float64(len(timer)))

		for _, pct := range percentiles {
			if len(timer) > 1 {
				var abs float64
				if pct.float >= 0 {
					abs = pct.float
				} else {
					abs = 100 + pct.float
				}
				// poor man's math.Round(x):
				// math.Floor(x + 0.5)
				indexOfPerc := int(math.Floor(((abs / 100.0) * float64(count)) + 0.5))
				if pct.float >= 0 {
					indexOfPerc -= 1 // index offset=0
				}
				if indexOfPerc < 0 {
					indexOfPerc = 0
				}
				maxAtThreshold = timer[indexOfPerc]
			}

			var tmpl string
			var pctstr string
			if pct.float >= 0 {
				tmpl = "%s.upper_%s%s %s %d\n"
				pctstr = pct.str
			} else {
				tmpl = "%s.lower_%s%s %s %d\n"
				pctstr = pct.str[1:]
			}

			if _, ok := timersFlags[bucketWithoutPostfix]; ok {
				fmt.Fprintf(buffer, tmpl, bucketWithoutPostfix, pctstr, *postfix, maxAtThreshold, now)
			}
		}

		mean_s := strconv.FormatFloat(mean, 'f', -1, 64)
		fmt.Fprintf(buffer, "%s.mean%s %f %d\n", bucketWithoutPostfix, *postfix, mean_s, now)

		if metricExists {
			for _, funcName := range metric.Functions {
				switch funcName {
				case "std":
					std_s := strconv.FormatFloat(stDeviation, 'f', -1, 64)
					fmt.Fprintf(buffer, "%s.std%s %f %d\n", bucketWithoutPostfix, *postfix, std_s, now)
				case "sum":
					sum_s := strconv.FormatFloat(sum, 'f', -1, 64)
					fmt.Fprintf(buffer, "%s.sum%s %d %d\n", bucketWithoutPostfix, *postfix, sum_s, now)
				case "sla_violations":
					fmt.Fprintf(buffer, "%s.sla_violations%s %d %d\n", bucketWithoutPostfix, *postfix, violationsCount, now)
				case "upper":
					max_s := strconv.FormatFloat(max, 'f', -1, 64)
					fmt.Fprintf(buffer, "%s.upper%s %d %d\n", bucketWithoutPostfix, *postfix, max_s, now)
				case "lower":
					min_s := strconv.FormatFloat(min, 'f', -1, 64)
					fmt.Fprintf(buffer, "%s.lower%s %d %d\n", bucketWithoutPostfix, *postfix, min_s, now)
				case "count":
					fmt.Fprintf(buffer, "%s.count%s %d %d\n", bucketWithoutPostfix, *postfix, count, now)
				}
			}
		}
		delete(timers, bucket)
	}
	return num
}

func (p *processor) AddTimerMetric(bucket string) {
  for _, m := range config.Metrics {
    if m.RegexpCompiled.MatchString(bucket) {
      timersFlags[bucket] = true
      timersMetrics[bucket] = m
      break
    }
  }
}

func (p *processor) SetConfig(configstr *[]byte) error {
	c := new(configuration)
	if err := json.Unmarshal(*configstr,&c); err !=nil {
		log15.Error("Error creating config", "err", err)
		return err
	}
	for i, m := range c.Metrics {
		compiled, err := regexp.Compile(m.Regexp)
		if err == nil {
			c.Metrics[i].RegexpCompiled = compiled
		}
	}
  p.config = c
  return nil
}

func NewProcessor() *processor {
  return new(processor)
}
