package main

import (
	"container/heap"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

//tracing every account's owner and money amount
var accountMap map[string]int

type ReceivedPriority struct {
	ReceivedCount   []int
	MaxPriorityItem *Item
}

//track every message's received priority
var MessageReceivedCount map[string]ReceivedPriority

func initMessageReceivedCount() {
	MessageReceivedCount = make(map[string]ReceivedPriority)
}
func initAccountMap() {
	accountMap = make(map[string]int)
}

func putMessage(message string, priority int) {
	priorityInQueue := -1 * priority
	item := &Item{
		Value:    message,
		Priority: priorityInQueue,
	}
	heap.Push(&MessageQueue, item)

	nodeNum := priority % 100

	receivedNodeArray := []int{nodeNum}

	receivedPriority := &ReceivedPriority{
		receivedNodeArray,
		item,
	}
	MessageReceivedCount[message] = *receivedPriority
	fmt.Println("test putMessage [", message, "item : ", item, "] in messageQueue and messageReceivedCount")
	testMessageQueue()
	if len(multicastGroup) == 1 {
		executeTransactionInQueue()
	}
}

func updateMessageReceivedCount(message string, priority int) bool {
	receivedPriority := MessageReceivedCount[message]
	nodeNum := priority % 100
	var receivedNodeArray []int
	if !contains(receivedPriority.ReceivedCount, nodeNum) {
		receivedNodeArray = append(receivedPriority.ReceivedCount, nodeNum)

	} else {
		receivedNodeArray = receivedPriority.ReceivedCount
	}

	// todo bug?
	fmt.Println("test updateMessageReceivedCount receivedNodeArray : message = ", message, " priority = ", priority)
	log.Println("test updateMessageReceivedCount receivedNodeArray : message = ", message, " priority = ", priority)

	// testMessageReceivedCount(receivedNodeArray)

	item := receivedPriority.MaxPriorityItem
	oldMaxPriority := -1 * item.Priority

	if oldMaxPriority < priority {
		item.Priority = -1 * priority
		newReceivedPriority := &ReceivedPriority{
			receivedNodeArray,
			item,
		}
		MessageReceivedCount[message] = *newReceivedPriority
		fmt.Println("test updateMessageReceivedCount : length of receivedNodeArray : ", len(MessageReceivedCount[message].ReceivedCount), "max priority :", MessageReceivedCount[message].MaxPriorityItem.Priority)
		log.Println("test updateMessageReceivedCount : length of receivedNodeArray : ", len(MessageReceivedCount[message].ReceivedCount), "max priority :", MessageReceivedCount[message].MaxPriorityItem.Priority)
		return true

	} else {
		newReceivedPriority := &ReceivedPriority{
			receivedNodeArray,
			item,
		}
		MessageReceivedCount[message] = *newReceivedPriority
		fmt.Println("test updateMessageReceivedCount : length of receivedNodeArray : ", len(MessageReceivedCount[message].ReceivedCount), "max priority :", MessageReceivedCount[message].MaxPriorityItem.Priority)
		log.Println("test updateMessageReceivedCount : length of receivedNodeArray : ", len(MessageReceivedCount[message].ReceivedCount), "max priority :", MessageReceivedCount[message].MaxPriorityItem.Priority)

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

//start execution only when messageQueue is not empty
//try to find executable transactionItem and deliver the transactionItem.
func executeTransactionInQueue() {
	if MessageQueue.Len() > 1 {
		// fmt.Println("test executeTransactionInQueue : ")

		// transactionItem := heap.Pop(&MessageQueue).(*Item)
		transactionItem := MessageQueue[0]

		nodeNumber := (transactionItem.Priority * -1) % 100
		if _, ok := multicastGroup[nodeNumber]; !ok {
			delete(MessageReceivedCount, transactionItem.Value)
			heap.Pop(&MessageQueue)
			return
		}

		flag := true

		messageReceivedNode := MessageReceivedCount[transactionItem.Value].ReceivedCount
		fmt.Println("test poped message : message = ", transactionItem.Value, " priority = ", transactionItem.Priority)
		log.Println("test poped message : message = ", transactionItem.Value, " priority = ", transactionItem.Priority)

		for k := range multicastGroup {
			if !contains(messageReceivedNode, k) {
				flag = false
				break
			}
		}
		fmt.Println("test executeTransactionInQueue : flag = ", flag)
		//if the received every node's priority of this message, deliver it. else put it back.
		if flag {
			messages := transactionItem.Value
			log.Println(getTimestamp(), "- Delivered: ", messages)
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
			delete(MessageReceivedCount, messages)
			heap.Pop(&MessageQueue)

		}
		// } else {
		// 	heap.Push(&MessageQueue, transactionItem)
		// }
	}
}

func deposit(client string, m string) {
	money, _ := strconv.Atoi(m)
	if val, ok := accountMap[client]; ok {
		accountMap[client] = money + val
	} else {
		accountMap[client] = money
	}
	printBalance()
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
			printBalance()
		}
	}
}

func printBalance() {
	fmt.Print("BALANCES ")
	keys := make([]string, 0, len(accountMap))
	for k := range accountMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	temp := ""
	for _, k := range keys {
		fmt.Print(k, ":", accountMap[k], " ")
		temp += k
		temp += ":"
		temp += strconv.Itoa(accountMap[k])
		temp += " "
	}
	fmt.Println()
	log.Println(getTimestamp(), "- BALANCES ", temp)
}
