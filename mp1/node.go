package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	Name     string
	Number   int
	Hostname string
	Port     string
	Conn     net.Conn
}

// PriorityMessage
//message with transaction and its priority = nodeNumber * 10 + timestamp
type PriorityMessage struct {
	Message  string
	Priority int // The priority of the item in the queue.
}

var ipAddress, _ = getIp()
var MessageQueue PriorityQueue
var localNodeNumber int
var timestamp int
var Flog *os.File

func main() {
	localNodeName := "node"
	localNodePort := ""
	configFileName := ""
	timestamp = 0
	initMulticastGroup()
	initBufferedMessageMap()
	initMessageReceivedCount()
	initAccountMap()
	MessageQueue.InitMessageQueue()
	allNodeConnected = false

	// ./mp1_node {node id} {port} {config file}
	if len(os.Args) > 1 {
		localNodeName += os.Args[1]
		var err error
		localNodeNumber, err = strconv.Atoi(os.Args[1])
		//temps := strings.TrimSuffix(localNodeName, "node")
		//var err error
		//localNodeNumber, err = strconv.Atoi(temps)
		if err != nil {
			fmt.Println("Wrong node name format: the node name should be a number, like '1'.")
			return
		}
	} else {
		localNodeName = "node1"
		localNodeNumber = 1
	}
	fmt.Println("node name:", localNodeName)
	fmt.Println("node number", localNodeNumber)

	if len(os.Args) > 2 {
		localNodePort = os.Args[2]
	} else {
		localNodePort = "1234"
	}
	fmt.Println("port:", localNodePort)

	if len(os.Args) > 3 {
		configFileName = os.Args[3]

	} else {
		err := "no config file."
		fmt.Println("Error: ", err)
		return
	}
	localNode := Node{
		Name:     localNodeName,
		Number:   localNodeNumber,
		Hostname: ipAddress,
		Port:     localNodePort,
	}
	fmt.Println("added local node (name : ", localNode.Name, ") to multicast group:")

	// setLog()
	filename := "Node" + strconv.Itoa(localNodeNumber) + "Result.log"
	file := "Node" + strconv.Itoa(localNodeNumber) + "Result.log"
	os.Remove(file)
	var err error
	Flog, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer Flog.Close()
	log.SetOutput(Flog)

	addToGroup(localNodeNumber, localNode)
	connectedNodeNumbers = append(connectedNodeNumbers, localNodeNumber)
	fmt.Println("test connectedNodeNumbers : connectedNodeNumbers.len =  ", len(connectedNodeNumbers))

	// read config file, build connection
	readConfigFile(configFileName)

	go ioScanner()
	address := ipAddress + ":" + localNodePort

	listener, err := net.Listen("tcp", address)
	fmt.Println("-----start listening-----", "local address:", address)
	if err != nil {
		fmt.Println("Error listening :", err.Error())
		return
	}

	// accept tcp connection from other nodes
	for {
		fmt.Println("receiver")
		conn, listenErr := listener.Accept()
		fmt.Println("test receive")
		if listenErr != nil {
			fmt.Println("error: accepting tcp connection:", listenErr.Error())
			return
		}
		fmt.Println("Connection accepted, remote address = ", conn.RemoteAddr())
		//wg.Add(1)
		fmt.Println("test go handleConnection")
		go handleConnection(conn)
	}
}

func ioScanner() {
	inputReader := bufio.NewReader(os.Stdin)
	for {
		// read message from stdin
		temp, _, err := inputReader.ReadLine()
		if err == io.EOF {
			fmt.Println("An error occurred on reading stdin.")
			return
		}
		input := string(temp)
		fmt.Println("read stdin:", input)
		incrementTimeStamp()
		priority := getLocalPriority()
		input += " "
		input += strconv.Itoa(priority)

		putMessage(input, priority)
		multicastInGroup(input, priority)
	}
}

//func getTimeStamp() int {return timestamp}

func incrementTimeStamp() { timestamp++ }

func getLocalPriority() int { return timestamp*100 + localNodeNumber }

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		dec := json.NewDecoder(reader)
		for dec.More() {
			var PriorityMessages PriorityMessage

			// decode an array value (PriorityMessage)
			err := dec.Decode(&PriorityMessages)
			if err != nil {
				fmt.Println("Error reading from: ", err.Error())
				// addr := conn.RemoteAddr().String()
				// deleteFromGroupUsingAddr(addr)
				return
			}
			fmt.Println("received  message :", PriorityMessages.Message, PriorityMessages.Priority)
			log.Println(getTimestamp(), "- received  message :", PriorityMessages.Message, PriorityMessages.Priority)
			processReceivedMessage(PriorityMessages.Message, PriorityMessages.Priority)
		}
	}
}

func readConfigFile(configFileName string) {
	configFile, configError := os.Open(configFileName)
	if configError != nil {
		fmt.Println("An error occurred on opening the config file: ", configError)
		return
	}
	configReader := bufio.NewReader(configFile)
	temp, _, err := configReader.ReadLine()
	if err == io.EOF {
		fmt.Println("An error occurred on opening the config file, empty file of missing argument.")
		return
	}
	s := string(temp)
	nodeNum, _ := strconv.Atoi(s)
	fmt.Println("node number: " + s)
	for i := 0; i < nodeNum; i++ {
		temp, _, err := configReader.ReadLine()
		if err == io.EOF {
			fmt.Println("An error occurred on reading the config file.")
			return
		}
		nodeMessage := string(temp)
		fmt.Println("node message:" + nodeMessage)
		tempMessage := strings.Fields(nodeMessage)
		name := tempMessage[0]
		temps := strings.TrimPrefix(name, "node")
		nodenum, _ := strconv.Atoi(temps)
		number := nodenum
		hostname := DNSResolution(tempMessage[1])
		port := tempMessage[2]
		node := Node{
			Name:     name,
			Number:   number,
			Hostname: hostname,
			Port:     port,
		}
		fmt.Println("node name:" + node.Name)
		addToGroup(number, node)
	}
	fmt.Println("completed reading from config file, multicast group loaded successfully")
	configFile.Close()
}

func getIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), err
			}
		}
	}
	return "", errors.New("can not find localhost ip address")
}

func DNSResolution(name string) string {
	hostIP, err := net.ResolveIPAddr("ip4", name)
	if err != nil {
		fmt.Println("DNS resolution error.")
	}
	return hostIP.String()
}

func getTimestamp() string {
	now := time.Now()
	nowSecond := now.Unix()
	nowNano := now.UnixNano() - nowSecond*1000000000
	nano := strconv.FormatInt(nowNano, 10)
	var nowMicro string
	if len(nano) > 6 {
		nowMicro = nano[0:6]
	} else {
		nowMicro = nano
	}
	return strconv.FormatInt(nowSecond, 10) + "." + nowMicro
}
