package stats

import (
	"encoding/json"
	"io"

	info "github.com/google/cadvisor/info/v1"
)

type AggregatedStats []AggregatedStat

type AggregatedStat struct {
	Id           string `json:"id,omitempty"`
	ResourceType string `json:"resource_type,omitempty"`
	*info.ContainerStats
}

func convertToAggregatedStats(id string, containerIds map[string]string, resourceType string, stats []info.ContainerInfo, memLimit uint64) []AggregatedStats {
	maxDataPoints := len(stats[0].Stats)
	totalAggregatedStats := []AggregatedStats{}

	for i := 0; i < maxDataPoints; i++ {
		aggregatedStats := []AggregatedStat{}
		for _, stat := range stats {
			if len(stat.Stats) > i {
				if resourceType == "container" && id == "" {
					id = stat.Name
				}
				statPoint := convertCadvisorStatToAggregatedStat(id, containerIds, resourceType, memLimit, stat.Stats[i])
				aggregatedStats = append(aggregatedStats, statPoint)
			}
		}
		totalAggregatedStats = append(totalAggregatedStats, aggregatedStats)
	}
	return totalAggregatedStats
}

func convertCadvisorStatToAggregatedStat(id string, containerIds map[string]string, resourceType string, memLimit uint64, stat *info.ContainerStats) AggregatedStat {
	if resourceType == "container" {
		if id[:len("/docker/")] == "/docker/" {
			id = id[len("/docker/"):]
		}
		if idVal, ok := containerIds[id]; ok {
			id = idVal
		}
	}
	return AggregatedStat{id, resourceType, stat}
}

func writeAggregatedStats(id string, containerIds map[string]string, resourceType string, infos []info.ContainerInfo, memLimit uint64, writer io.Writer) error {
	if resourceType == "container" {
		if _, ok := containerIds[id]; !ok {
			return nil
		}
	}
	aggregatedStats := convertToAggregatedStats(id, containerIds, resourceType, infos, memLimit)
	for _, stat := range aggregatedStats {
		data, err := json.Marshal(stat)
		if err != nil {
			return err
		}

		writer.Write(data)
		writer.Write([]byte("\n"))
	}

	return nil
}
