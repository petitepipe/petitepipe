package main

import (
	"container/heap"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//tracing every account's owner and money amount
var accountMap map[string]int

type ReceivedPriority struct {
	receivedCount []int
	maxPriority   int
}

//track every message's received priority
var messageReceivedCount map[string]ReceivedPriority

func initMessageReceivedCount() {
	messageReceivedCount = make(map[string]ReceivedPriority)
}
func initAccountMap() {
	accountMap = make(map[string]int)
}

func putMessage(message string, priority int) {
	priorityInQueue := -1 * priority
	item := &Item{
		value:    message,
		priority: priorityInQueue,
	}
	heap.Push(&messageQueue, item)

	nodeNum := priority % 100


	receivedNodeArray := []int{nodeNum}
	receivedPriority := &ReceivedPriority{
		receivedNodeArray,
		priority,
	}
	messageReceivedCount[message] = *receivedPriority
	fmt.Println("test putMessage [", message, "priority : ", priority, "] in messageQueue and messageReceivedCount")
	testMessageQueue()
	if len(multicastGroup) == 1 {
		executeTransactionInQueue()
	}
}

func updateMessageReceivedCount(message string, priority int) bool {
	receivedPriority := messageReceivedCount[message]
	nodeNum := priority % 100
	var receivedNodeArray []int
	if !contains(receivedPriority.receivedCount, nodeNum) {
		receivedNodeArray = append(receivedPriority.receivedCount, nodeNum)

	} else {
		receivedNodeArray = receivedPriority.receivedCount
	}

	// todo bug?
	fmt.Println("test updateMessageReceivedCount receivedNodeArray : message = ", message, " priority = ", priority)
	testMessageReceivedCount(receivedNodeArray)

	if receivedPriority.maxPriority < priority {
		newReceivedPriority := &ReceivedPriority{
			receivedNodeArray,
			priority,
		}
		messageReceivedCount[message] = *newReceivedPriority
		fmt.Println("test updateMessageReceivedCount : length of receivedNodeArray : ", len(messageReceivedCount[message].receivedCount), "max priority :", messageReceivedCount[message].maxPriority)
		return true
	} else {
		newReceivedPriority := &ReceivedPriority{
			receivedNodeArray,
			receivedPriority.maxPriority,
		}
		messageReceivedCount[message] = *newReceivedPriority
		fmt.Println("test updateMessageReceivedCount : length of receivedNodeArray : ", len(messageReceivedCount[message].receivedCount), "max priority :", messageReceivedCount[message].maxPriority)
		return false
	}
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func testMessageReceivedCount(messageReceivedNode []int) {
	for i := 0; i < len(messageReceivedNode); i++ {
		fmt.Println("testMessageReceivedCount messageReceivedNode : i = ", i)
		fmt.Println("testMessageReceivedCount messageReceivedNode : ", messageReceivedNode[i])
	}
}

//start execution only when messageQueue is not empty
//try to find executable transactionItem and deliver the transactionItem.
func executeTransactionInQueue() {
	if messageQueue.Len() > 1 {
		// fmt.Println("test executeTransactionInQueue : ")

		transactionItem := heap.Pop(&messageQueue).(*Item)
		nodeNumber := (transactionItem.priority * -1) % 100
		if _, ok := multicastGroup[nodeNumber]; !ok {
			delete(messageReceivedCount, transactionItem.value)
			return
		}

		flag := true

		messageReceivedNode := messageReceivedCount[transactionItem.value].receivedCount
		fmt.Println("test MessageReceivedCount receivedNodeArray : message = ", transactionItem.value, " priority = ", transactionItem.priority)
		testMessageReceivedCount(messageReceivedNode)

		for k := range multicastGroup {
			if !contains(messageReceivedNode, k) {
				flag = false
				break
			}
		}
		fmt.Println("test executeTransactionInQueue : flag = ", flag)
		//if the received every node's priority of this message, deliver it. else put it back.
		if flag {
			messages := transactionItem.value
			messageArray := strings.Split(messages, "/n")
			for _, message := range messageArray {
				if strings.Contains(message, "DEPOSIT") {
					info := strings.Split(message, " ")
					if cap(info) < 3 {
						fmt.Println("illegal transaction: ", message)
						continue
					}
					deposit(info[1], info[2])
					fmt.Println("delivered transaction: ", message)

				} else if strings.Contains(message, "TRANSFER") {
					info := strings.Split(message, " ")
					if cap(info) < 5 {
						fmt.Println("illegal transaction: ", message)
						continue
					}
					transfer(info[1], info[3], info[4])
					fmt.Println("delivered transaction: ", message)
				}
			}
			delete(messageReceivedCount, messages)
			printBalance()
		} else {
			heap.Push(&messageQueue, transactionItem)
		}
	}
}

func deposit(client string, m string) {
	money, _ := strconv.Atoi(m)
	if val, ok := accountMap[client]; ok {
		accountMap[client] = money + val
	} else {
		accountMap[client] = money
	}
	return
}

func transfer(sender string, receiver string, m string) {
	money, _ := strconv.Atoi(m)
	if valSender, isPre := accountMap[sender]; isPre {
		if accountMap[sender] >= money {
			if valReceiver, isPresent := accountMap[receiver]; isPresent {
				accountMap[receiver] = money + valReceiver
				accountMap[sender] = valSender - money
			} else {
				accountMap[receiver] = money
			}
		}
	}
	return
}

func printBalance() {
	fmt.Print("BALANCES ")
	keys := make([]string, 0, len(accountMap))
	for k := range accountMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Print(k, ":", accountMap[k], " ")
	}
	fmt.Println()
}
