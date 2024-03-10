package jobs

import (
	"fmt"
	"go_node_engine/model"
	// "math"
	"sync"
	"time"
)

var once sync.Once

const CPU_DIFF_THRESHOLD = 1 // in percentage
const MEM_DIFF_THRESHOLD = 1 // in percentage
var lastCpuUsage float64 = -CPU_DIFF_THRESHOLD
var lastMemUsage float64 = -MEM_DIFF_THRESHOLD

var cadence = time.Millisecond * 2000
var initialCadence float64

var msgSeq = 0

func NodeStatusUpdater(statusUpdateHandler func(node model.Node), initialCadence int) {
	cadence = time.Duration(initialCadence) * time.Microsecond
	// initialCadence = initialCadence
	// updateRoutine(statusUpdateHandler)

	once.Do(func() {
	 go updateRoutine(statusUpdateHandler)
	})
}

func updateRoutine(statusUpdateHandler func(node model.Node)) {
	for true {
		select {
		case <-time.After(cadence):
			nodeInfo := model.GetDynamicInfo()
			nodeInfo.MessageSeq = msgSeq
			msgSeq++
			// if math.Abs(nodeInfo.CpuUsage-lastCpuUsage) > CPU_DIFF_THRESHOLD || math.Abs(nodeInfo.MemoryUsed-lastMemUsage) > MEM_DIFF_THRESHOLD {
			// 	UpdateCadence(time.Duration(math.Round(initialCadence*1.2)) * time.Microsecond)
			// } else {
			// 	UpdateCadence(time.Duration(math.Round(initialCadence*0.8)) * time.Microsecond)
			// }
			statusUpdateHandler(nodeInfo)
			lastCpuUsage = nodeInfo.CpuUsage
			lastMemUsage = nodeInfo.MemoryUsed

			// rtnl, err := tc.Open(&tc.Config{})
			// if err != nil {
			// 	fmt.Fprintf(os.Stderr, "could not open rtnetlink socket: %v\n", err)
			// 	return
			// }
			// defer func() {
			// 	if err := rtnl.Close(); err != nil {
			// 		fmt.Fprintf(os.Stderr, "could not close rtnetlink socket: %v\n", err)
			// 	}
			// }()

			// For enhanced error messages from the kernel, it is recommended to set
			// option `NETLINK_EXT_ACK`, which is supported since 4.12 kernel.
			//
			// If not supported, `unix.ENOPROTOOPT` is returned.

			// err = rtnl.SetOption(netlink.ExtendedAcknowledge, true)
			// if err != nil {
			// 	fmt.Fprintf(os.Stderr, "could not set option ExtendedAcknowledge: %v\n", err)
			// 	return
			// }

			// get all the qdiscs from all interfaces
			// qdiscs, err := rtnl.Qdisc().Get()
			// if err != nil {
			// 	fmt.Fprintf(os.Stderr, "could not get qdiscs: %v\n", err)
			// 	return
			// }

			// for _, qdisc := range qdiscs {
			// 	iface, err := net.InterfaceByIndex(int(qdisc.Ifindex))
			// 	if err != nil {
			// 		fmt.Fprintf(os.Stderr, "could not get interface from id %d: %v", qdisc.Ifindex, err)
			// 		return
			// 	}
			// 	fmt.Printf("%20s\t%d\n", iface.Name, qdisc.Stats.Qlen)
			// }

		}
	}
}

func UpdateCadence(newCadence time.Duration) {
	// fmt.Println("updating cadence to node")
	// fmt.Println(newCadence)
	if cadence != newCadence {
		fmt.Println("changing node cadence")
	}
	cadence = newCadence
}
