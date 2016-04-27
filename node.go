package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	rand2 "math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ChordRing struct {
	Port          string
	Successor     string
	Predecessor   string
	Active        bool
	FingerTable   []string
	Next          int
	nSuccessors   []string
	mutex         sync.Mutex
	Bucket        map[string]string
}

type Nothing struct{}

func (CR *ChordRing) port(line string) error {
	if len(line) < 1 {
		log.Println(line, "is not a valid port or address")
		return nil
	}
	CR.Port = line
	fmt.Println("Port is now:			" + CR.Port)
	return nil
}

func (CR *ChordRing) GetPredecessor(_ string, reply *string) error {
	*reply = CR.Predecessor
	return nil
}

func (CR *ChordRing) GetSuccessorList(_ Nothing, reply *[]string) error {
	*reply = CR.nSuccessors
	return nil
}

func (CR *ChordRing) fixSuccessor() {
	CR.nSuccessors = CR.nSuccessors[1:]
	CR.nSuccessors = append(CR.nSuccessors, CR.Successor)
}

func (CR *ChordRing) stabilize() error {
	var x string
	if err := CR.call(CR.Successor, "ChordRing.GetPredecessor", "", &x); err != nil {
		CR.Successor = CR.nSuccessors[0]
		CR.nSuccessors = CR.nSuccessors[1:]

		fmt.Println("Stabilize:\n", err)
		return nil
	}
	if x != "" && between(hashString(CR.Port), hashString(x), hashString(CR.Successor), false) {
		fmt.Println("Stabilizing Ring...		Successors List Changed")
		CR.Successor = x
	}
	CR.notifyRequest()
	var eh Nothing
	CR.call(CR.Successor, "ChordRing.GetSuccessorList", eh, &CR.nSuccessors)
	CR.fixSuccessor()
	return nil
}

func (CR *ChordRing) create() error {
	if CR.Active == true {
		return nil
	}
	fmt.Println("New Node entering the network...")
	CR.Active = true
	CR.Predecessor = ""
	CR.Successor = CR.Port
	rpc.Register(CR)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", CR.Port)
	if err != nil {
		log.Fatal("Error! ", err)
	}
	go http.Serve(listener, nil)
	fmt.Println("Starting to listen on port:	", CR.Port)
	var reply Nothing
	go func() {
		for {
			CR.stabilize()
			CR.checkPredecessorRequest()
			time.Sleep(time.Second)
			}
		}()
	go func() {
		for {
			CR.fixFingers(&reply, &reply)
			time.Sleep(time.Second)


		}

	}()
	return nil
}

func (CR *ChordRing) FindSuccessor(id *big.Int, reply *string) error {
	if between(hashString(CR.Port), id, hashString(CR.Successor), true) {
		*reply = CR.Successor
		return nil
	} else {
		nprime := CR.closestPreceedingNode(id)
		CR.call(nprime, "ChordRing.FindSuccessor", id, reply)
		return nil
	}
}

func (CR *ChordRing) closestPreceedingNode(id *big.Int) string {
	for i := FingerSize; i >= 1; i-- {
		if CR.FingerTable[i] == "" {
			continue
		}
		if between(hashString(CR.Port), hashString(CR.FingerTable[i]), id, false) {
			return CR.FingerTable[i]
		}
	}
	return CR.Successor
}
func (CR *ChordRing) join(line string) error {
	var reply string
	CR.create()
	ip := line
	if err := CR.call(ip, "ChordRing.FindSuccessor", hashString(CR.Port), &reply); err != nil {
		log.Println(err)
		return nil
	}
	fmt.Println("Successor is 			" + reply)
	CR.Successor = reply
	go func() {
		time.Sleep(time.Second)
		CR.getAllRequest()
	}()
	return nil
}

func (CR *ChordRing) help() error {
	fmt.Println("\nValid commands:")
	fmt.Println("port <n>")
	fmt.Println("join <address>")
	fmt.Println("put <key>, <value>, <address>(optional)")
	fmt.Println("test <number of test keys>")
	fmt.Println("get <key>")
	fmt.Println("delete <key>")
	fmt.Println("dump")
	fmt.Println("printkeys")
	fmt.Println("dumpaddr <address>")
	fmt.Println("dumpall")
	fmt.Println("quit")
	return nil
}

func (CR *ChordRing) quit() error {
	CR.putAllRequest("")
	os.Exit(0)
	return nil
}

func (CR *ChordRing) Delete(args []string, reply *string) error {
	key := args[0]
	sender := args[1]
		if _, exists := CR.Bucket[key]; exists {
			delete(CR.Bucket, key)
			*reply = "Key @" + key + " deleted. 		 Hosted by " + appendLocalHost(CR.Port)
			fmt.Println("Key @"+key+" deleted by	       		=>", sender)
			return nil
		} else if CR.Port == args[2] && len(args[3]) > 1 {
			fmt.Println(CR.Port + " " + CR.Successor + "  " + args[2])
		} else {
			ip := CR.Successor
			if args[3] == "" {
				args[3] = "done"
			}
			args[1] = CR.Successor
			if err := CR.call(ip, "ChordRing.Delete", args, &reply); err != nil {
				log.Println(err)
				return nil
			}	
		}
	*reply = "Key doesn't exist."
	return nil
}

func (CR *ChordRing) deleteRequest(args []string) error {
	var reply string
	original := CR.Port
	ip := ""
	CR.FindSuccessor(hashString(args[1]), &ip)
	args = []string{args[1], original, ip, ""}
	if err := CR.call(ip, "ChordRing.Delete", args, &reply); err != nil {
		log.Println(err)
		return nil
	}
	fmt.Println(reply)
	return nil
}

func (CR *ChordRing) Get(args []string, reply *string) error {
		if value, exists := CR.Bucket[args[0]]; exists {
			*reply = "Key @" + args[0] + " => " + value + "			 " + appendLocalHost(CR.Port)
			return nil
		} else if CR.Port == args[2] && len(args[3]) > 1 {
			fmt.Println(CR.Port + " " + CR.Successor + "  " + args[2])
			return nil
		} else {
			if args[3] == "" {
				args[3] = "true"
				fmt.Println(args[3])
			}
			ip := CR.Successor
			args[1] = CR.Successor
			if err := CR.call(ip, "ChordRing.Get", args, &reply); err != nil {
				log.Println(err)
				return nil
			}
		}
	//*reply = "The data does not exist"
	return nil
}
func (CR *ChordRing) getRequest(args []string) error {
	var reply string
	ip := ""
	CR.FindSuccessor(hashString(args[1]), &ip)
	original := CR.Port
	args = []string{args[1], original, ip, ""}
	if err := CR.call(ip, "ChordRing.Get", args, &reply); err != nil {
		log.Println(err)
		return nil
	}
	fmt.Println("Sending Request...")
	fmt.Println(reply)
	return nil
}

func (CR *ChordRing) GetAll(_ string, reply *map[string]string) error {
	*reply = CR.Bucket
	return nil
}

func (CR *ChordRing) getAllRequest() {
	var reply map[string]string
	err := CR.call(CR.Successor, "ChordRing.GetAll", "", &reply)
	if err != nil {
		log.Println("Could not retrieve data from successor")
	}
	for key, value := range reply {
		if between(hashString(CR.Port), hashString(key), hashString(CR.Successor), false) {
			CR.Bucket[key] = value
			fmt.Println("Transferring Data:\n				Key@" + key + "=> " + value)
		var reply2 string
		args  := []string{key, CR.Port}
		if err := CR.call(CR.Successor, "ChordRing.Delete", args, &reply2); err != nil {
			log.Println(err)
			}
		}
	}

}

func (CR *ChordRing) Put(args []string, reply *string) error {
	key := args[0]
	value := args[1]
	CR.Bucket[key] = value
	if CR.Bucket[key] == value {
		*reply = "Key @" + key + " => " + value + "			  " + appendLocalHost(CR.Port)
		return nil
	}
	*reply = "Could not add"
	return nil
}

func (CR *ChordRing) putRequest(args []string) error {
	var reply string
	key := args[1]
	value := args[2]
	ip := ""
	CR.FindSuccessor(hashString(key), &ip)
	args = []string{key, value, CR.Port}
	if err := CR.call(ip, "ChordRing.Put", args, &reply); err != nil {
		log.Println(err)
		return nil
	}
	fmt.Println("\nSending Request...")
	fmt.Println(reply)
	return nil
}
func (CR *ChordRing) putAllRequest(_ string) {
	reply := ""
	CR.call(CR.Successor, "ChordRing.PutAll", CR.Bucket, &reply)
}

func (CR *ChordRing) PutAll(data map[string]string, _ *string) error {
	for key, value := range data {
		CR.Bucket[key] = value
	}
	return nil
}
func (CR *ChordRing) test(str string) error {
	s1 := rand2.NewSource(time.Now().UnixNano())
    	r1 := rand2.New(s1)
	n, err := strconv.Atoi(str)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key := strconv.Itoa(r1.Intn(9999))
		value := randString(r1.Intn(10))
		args := []string{CR.Port, key, value}
		CR.putRequest(args)
	}
	return nil
}

func (CR *ChordRing) CheckPredecessor(_ string, reply *bool) error {
	*reply = true
	return nil
}

func (CR *ChordRing) checkPredecessorRequest() {
	if CR.Predecessor != "" {
		reply := false
		if err := CR.call(CR.Predecessor, "ChordRing.CheckPredecessor", "", reply); err != nil {
			fmt.Println("Could not contact predecessor")
			CR.Predecessor = ""
		}
	}
}

func (CR *ChordRing) Notify(n1 string, reply *string) error {
	if CR.Predecessor == "" || between(hashString(CR.Predecessor), hashString(n1), hashString(CR.Port), false) {
		CR.Predecessor = n1
		fmt.Println("Notify: Predecessor is 		", CR.Predecessor)
		*reply = "You are now my predecessor. Sent from		" + CR.Port
	}
	return nil
}

func (CR *ChordRing) notifyRequest() error {
	reply := ""
	ip := CR.Successor
	if err := CR.call(ip, "ChordRing.Notify", CR.Port, &reply); err != nil {
		fmt.Println("Notfify:", err)
		return nil
	} else {

	}
	return nil
}

func (CR *ChordRing) call(address string, method string, request interface{}, reply interface{}) error {

	conn, err := rpc.DialHTTP("tcp", appendLocalHost(address))

	if err != nil {
		return err
	}

	defer conn.Close()
	conn.Call(method, request, reply)

	return nil
}

func (CR *ChordRing) dump() error {
	pred := hashString(CR.Predecessor)
	pred2 := fmt.Sprint(pred)[:10]
	port := hashString(CR.Port)
	port2 := fmt.Sprint(port)[:10]
	succ := hashString(CR.Successor)
	succ2 := fmt.Sprint(succ)[:10]
	fmt.Println("Dump:   		 	 Node", CR.Port)
	if CR.Active {
		fmt.Println("			 	 This Node is active on the network")
	}
	fmt.Println("--------------\n|Neighborhood|\n--------------")
	fmt.Println("Predecessor: 			", CR.Predecessor, pred2)
	fmt.Println("Self:        			", CR.Port, port2)
	fmt.Println("Successor:   			", CR.Successor, succ2)
	n := len(CR.Bucket)
	if n > 0 {
		fmt.Println("------------\n|Data Items|\n------------")
		fmt.Println("				Data Exists--type 'printkeys'")
	}
	fmt.Println("--------------\n|Finger Table|\n--------------")
	for i := 0; i < 10; i++ {
		if i > 0 {
			if CR.FingerTable[FingerSize - i] != CR.FingerTable[FingerSize - (i +1)] {
				fmt.Println(CR.FingerTable[FingerSize - i], " => ", "[", FingerSize-i, "]")
			}
		} else {
			fmt.Println(CR.FingerTable[FingerSize - i], " => ", "[", FingerSize-i, "]")
		}
	}
	return nil
}

func (CR *ChordRing) dumpkeys() error {
	n := len(CR.Bucket)
	if n > 0 {
	fmt.Println("------------\n|Data Items|\n------------")
		for k, v := range CR.Bucket {
			fmt.Println("			 	 Key @ ", k, " => ", v)
		}
	}
	return nil
}

func (CR *ChordRing) fixFingers(request, _ *Nothing) error {
	CR.FingerTable[1] = CR.Successor
	CR.Next++
	if CR.Next > FingerSize {
		CR.Next = 1
	}
	reply := ""
	err := CR.FindSuccessor(jump(CR.Next, CR.Port), &reply)
	if err != nil {
		log.Println("error fixing fingers")
		return nil
	}
	CR.FingerTable[CR.Next] = reply
	for  {
		CR.Next++
		if between(hashString(CR.Port), jump(CR.Next, CR.Port), hashString(reply), false) {
			CR.FingerTable[CR.Next] = reply
		} else {
			break
		}
	}
	CR.Next--
	return nil
}

func readlines(CR *ChordRing) {
	for {
		//read the messages and split on newline
		buffer := bufio.NewReader(os.Stdin)
		str, err := buffer.ReadString('\n')
		line := ""
		if err != nil {
			log.Printf("Error: %q\n", err)
		}
		//strip the whitespace
		line = strings.TrimSpace(str)
		//separate the words into a slice and index == keywords
		elements := strings.Fields(line)
		if strings.HasPrefix(line, "create") {
			CR.create()
		} else if strings.HasPrefix(line, "put") {
			CR.putRequest(elements)
		} else if strings.HasPrefix(line, "get") {
			CR.getRequest(elements)
		} else if strings.HasPrefix(line, "join") {
			CR.join(elements[1])
		} else if strings.HasPrefix(line, "help") {
			CR.help()
		} else if strings.HasPrefix(line, "quit") {
			CR.quit()
		} else if strings.HasPrefix(line, "dump") {
			CR.dump()
		} else if strings.HasPrefix(line, "printkeys") {
			CR.dumpkeys()
		} else if strings.HasPrefix(line, "delete") {
			CR.deleteRequest(elements)
		} else if strings.HasPrefix(line, "port") {
			CR.port(elements[1])
		} else if strings.HasPrefix(line, "test") {
			CR.test(elements[1])
		} else {
			log.Println("\n\nError: " + "'" + elements[0] + "'" + " not a valid command. \n" + "Please type 'help' for a list of commands. \n")
		}
	}
}
func randString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphabet[b%byte(len(alphabet))]
	}
	return string(bytes)
}

func appendLocalHost(s string) string {
	addy := getLocalAddress()
	if strings.HasPrefix(s, ":") {
		return addy + s
	} else if strings.Contains(s, ":") {
		return s
	} else {
		return ""
	}
}

func main() {
	CR := new(ChordRing)
	CR.nSuccessors = make([]string, 3)
	CR.Bucket = make(map[string]string)
	CR.Port = ":3410"
	CR.FingerTable = make([]string, FingerSize+1)
	CR.Next = 0
	readlines(CR)
}
