package jobs

import (
	"go_node_engine/model"
	"sync"
	"time"
)

var once sync.Once

const CPU_THRESHOLD = 0.2 // in percentage
const MEM_THRESHOLD = 6   // in percentage

func NodeStatusUpdater(cadence time.Duration, statusUpdateHandler func(node model.Node)) {
	once.Do(func() {
		go updateRoutine(cadence, statusUpdateHandler)
	})
}

func updateRoutine(cadence time.Duration, statusUpdateHandler func(node model.Node)) {
	// TODO: use a better threshold strategy here
	for true {
		select {
		case <-time.After(cadence):
			nodeInfo := model.GetDynamicInfo()
			// if nodeInfo.CpuUsage > CPU_THRESHOLD || nodeInfo.MemoryUsed > MEM_THRESHOLD {
			statusUpdateHandler(nodeInfo)
			// }
		}
	}
}
