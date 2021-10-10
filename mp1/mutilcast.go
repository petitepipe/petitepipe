package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

//multicastGroup nodeNumber = key, node = value
var multicastGroup map[int]Node

var allNodeConnected bool
var connectedNodeNumbers []int

var bufferedMessageMap map[int][]PriorityMessage

func initMulticastGroup() {
	multicastGroup = make(map[int]Node)
}
func initBufferedMessageMap() {
	bufferedMessageMap = make(map[int][]PriorityMessage)
}

//every node include itself should be added into group
func addToGroup(nodeNumber int, node Node) {
	multicastGroup[nodeNumber] = node
	fmt.Println("added ", node.Name, " to the multicast group")
	log.Println(getTimestamp(), "- added ", node.Name, " to the multicast group")
}

//delete from the group if connection is broken
func deleteFromGroup(node Node) {
	delete(multicastGroup, node.Number)
	fmt.Println("removed ", node.Name, " from the multicast group")
	log.Println(getTimestamp(), "- removed ", node.Name, " from the multicast group")
	fmt.Println("multicastgroup length = ", len(multicastGroup))
}

// func deleteFromGroupUsingAddr(addr string) {
// 	for i := range multicastGroup {
// 		fmt.Println("test deleteFromGroupUsingAddr : ", "addr = ", addr, " , multicastGroup[i].Hostname = ", multicastGroup[i].Hostname)
// 		if strings.Contains(addr, multicastGroup[i].Hostname) {
// 			deleteFromGroup(multicastGroup[i])
// 			return
// 		}
// 	}
// }

func multicastInGroup(input string, priority int) {
	//fmt.Println( "test multicastInGroup msg :" , input)
	for k := range multicastGroup {
		if k != localNodeNumber {

			if multicastGroup[k].Conn == nil {
				fmt.Println("test multicastInGroup, multicastGroup.len = ", len(multicastGroup), ". allNodeConnected = ", allNodeConnected)
				tryDial(input, priority, multicastGroup[k])
				continue
			} else {
				// fmt.Println("test conn : remoteAddr :", multicastGroup[k].Conn.RemoteAddr())
				if _, ok := bufferedMessageMap[multicastGroup[k].Number]; ok {
					fmt.Println("test multicastingroup sending buffered message")
					sendBufferedMessage(multicastGroup[k])
				}
				send(input, priority, multicastGroup[k])
				//when start dialing to a node, send buffered message, and delete bufferedMsg for this node

			}
		}
	}
}

func tryDial(input string, priority int, node Node) {
	conn, sendingErr := net.Dial("tcp", node.Hostname+":"+node.Port)
	bufferMessage(PriorityMessage{input, priority}, node.Number)
	if sendingErr != nil {
		failureDetection(node, sendingErr)
		return
	}
	newNode := &Node{
		Name:     node.Name,
		Number:   node.Number,
		Hostname: node.Hostname,
		Port:     node.Port,
		Conn:     conn,
	}
	multicastGroup[node.Number] = *newNode
	fmt.Println("test conn : remoteAddr :", conn.RemoteAddr())
	fmt.Println("Dial success: added connection in:", node.Name)
	// once connected to a new node, report it in connectedNodeNumbers, and check if allNodeConnected
	// use as allNodeConnected(bool) a flag
	checkAllConnectCompleted(node.Number)
}

// todo log sent
func send(input string, priority int, node Node) {
	enc := json.NewEncoder(node.Conn)
	encodeErr := enc.Encode(PriorityMessage{input, priority})
	if encodeErr != nil {
		fmt.Println("encode error:", encodeErr)
		failureDetection(node, encodeErr)
		return
	}
	fmt.Println("sent msg: ", input, priority)
	log.Println(getTimestamp(), "- sent msg: ", input, priority)
}

func sendBufferedMessage(node Node) {
	bufferedMsg := bufferedMessageMap[node.Number]
	for i := range bufferedMsg {
		priorityMsg := bufferedMsg[i]
		fmt.Println("test sendBufferedMessage : ", priorityMsg.Message, priorityMsg.Priority)
		send(priorityMsg.Message, priorityMsg.Priority, node)
	}
	delete(bufferedMessageMap, node.Number)
	fmt.Println("deleted ", node.Name, " from bufferedMessageMap, buffered message sent successfully. ")
}

// todo log
func failureDetection(node Node, err error) {
	if allNodeConnected {
		fmt.Println("failure detected - error reading from: ", node.Name, " , error : ", err.Error())
		log.Println(getTimestamp(), "- failure detected - error reading from: ", node.Name, " , error : ", err.Error())
		deleteFromGroup(node)
	}
}

func checkAllConnectCompleted(nodeNumber int) {
	if !contains(connectedNodeNumbers, nodeNumber) {
		connectedNodeNumbers = append(connectedNodeNumbers, nodeNumber)
		fmt.Println("test checkAllConnectCompleted : connectedNodeNumbers.len =  ", len(connectedNodeNumbers))
		if len(connectedNodeNumbers) == len(multicastGroup) {
			allNodeConnected = true
			fmt.Println("checkAllConnectCompleted : allNodeConnected")
		}
	}
}

func bufferMessage(priorityMessage PriorityMessage, nodeNumber int) {
	if buffered, ok := bufferedMessageMap[nodeNumber]; ok {
		newBuffered := append(buffered, priorityMessage)
		bufferedMessageMap[nodeNumber] = newBuffered
		fmt.Println("test updating Buffered , ", "content = ", priorityMessage.Message, priorityMessage.Priority)

	} else {
		var newBuffered = []PriorityMessage{priorityMessage}
		//newBuffered = append(newBuffered, priorityMessage)
		bufferedMessageMap[nodeNumber] = newBuffered
		fmt.Println("test creating newBuffered , ", "content = ", priorityMessage.Message, priorityMessage.Priority)
	}
	fmt.Println("sending error: not build connection with node", nodeNumber, ", buffered message")
}

func testMessageQueue() {
	for i := 0; i < len(messageQueue); i++ {
		// fmt.Println("test messageQueue : i = ", i)
		// fmt.Println("test messageQueue : ", messageQueue[i].value, messageQueue[i].priority, "index : ", i)
	}
}

//todo debug update
func processReceivedMessage(message string, priority int) {
	//put the message and its corresponding nodeNumber to messageReceivedCount.
	//modify the messageQueue correspondingly
	//as the priority queue pop() in descending order, the priority queue should be  -1 * priority
	if _, ok := messageReceivedCount[message]; ok {
		changedMaxPriority := updateMessageReceivedCount(message, priority)
		if changedMaxPriority {
			priorityInQueue := -1 * messageReceivedCount[message].maxPriority
			item := &Item{
				value:    message,
				priority: priorityInQueue,
			}
			fmt.Println("test changedMaxPriority : ", message, "priority : ", priorityInQueue)
			log.Println("test changedMaxPriority : ", message, "priority : ", priorityInQueue)
			// testMessageQueue()

			messageQueue.update(item, item.value, priorityInQueue)
		}
	} else {
		//if the message is never received, push it into messageQueue and messageReceivedCount
		putMessage(message, priority)

		//if the message is sent by local node, putMessage only
		//if the message is sent by other node, putMessage, increment timestamp and multicastInGroup
		receivedNodeNumber := priority % 100
		if receivedNodeNumber != localNodeNumber {
			incrementTimeStamp()
			updatedLocalPriority := getLocalPriority()
			processReceivedMessage(message, updatedLocalPriority)
			multicastInGroup(message, updatedLocalPriority)
		}
	}
	// fmt.Println("test processReceivedMessage - executeTransactionInQueue : ", message, "priority : ", priority)
	executeTransactionInQueue()
}
