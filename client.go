package main  

  
import (  
    "fmt"  
    "hash/crc32"  
    "sort"     
    "net/http"
    "encoding/json" 
    "io/ioutil"
    "os"
    "strings"
)  
   


type KEY_VALUE struct{
    Key int `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}

type HASH_CIRC []uint32  


 type Node struct {  
    Id       int  
    IP       string    
}  
  
func NEW_NODE(id int, ip string) *Node {  
    return &Node{  
        Id:       id,  
        IP:       ip,  
    }  
}   

  
type CONSISTENT_HASH struct {  
    Nodes       map[uint32]Node  
    IsPresent   map[int]bool  
    Circle      HASH_CIRC  
    
}  
  
func NEW_CONSISTENT_HASH() *CONSISTENT_HASH {  
    return &CONSISTENT_HASH{  
        Nodes:     make(map[uint32]Node),   
        IsPresent: make(map[int]bool),  
        Circle:      HASH_CIRC{},  
    }  
} 



 // FUNCTION FOR ADDING NEW METHOD

func (hr *CONSISTENT_HASH) ADD_NEW_NODE(node *Node) bool {  
 
    if _, ok := hr.IsPresent[node.Id]; ok {  
        return false  
    }  
    str := hr.RETURN_IP(node)  
    hr.Nodes[hr.GET_HV(str)] = *(node)
    hr.IsPresent[node.Id] = true  
    hr.SORT_HASH_CIRCLE()  
    return true  
}  
  
func (hr *CONSISTENT_HASH) SORT_HASH_CIRCLE() {  
    hr.Circle = HASH_CIRC{}  
    for k := range hr.Nodes {  
        hr.Circle = append(hr.Circle, k)  
    }  
    sort.Sort(hr.Circle)  
}  
  
func (hr *CONSISTENT_HASH) RETURN_IP(node *Node) string {  
    return node.IP 
}  
  
func (hr *CONSISTENT_HASH) GET_HV(key string) uint32 {  
    return crc32.ChecksumIEEE([]byte(key))  
}  

//BREAKPOINT  
func (hr *CONSISTENT_HASH) Get(key string) Node {  
    hash := hr.GET_HV(key)  
    i := hr.NODE_SEARCH(hash)  
    return hr.Nodes[hr.Circle[i]]  
}  
 // SEARCH FOR NODE< GET A PARTICULR KEY VALUE PAIR
func (hr *CONSISTENT_HASH) NODE_SEARCH(hash uint32) int {  
    i := sort.Search(len(hr.Circle), func(i int) bool {return hr.Circle[i] >= hash })  
    if i < len(hr.Circle) {  
        if i == len(hr.Circle)-1 {  
            return 0  
        } else {  
            return i  
        }  
    } else {  
        return len(hr.Circle) - 1  
    }  
}  
 
 // PUT KEY METHOD 
func PUT_KEY(circle *CONSISTENT_HASH, str string, input string){
        ipAddress := circle.Get(str)  
        address := "http://"+ipAddress.IP+"/keys/"+str+"/"+input
		fmt.Println(address)
        req,err := http.NewRequest("PUT",address,nil)
        client := &http.Client{}
        resp, err := client.Do(req)
        if err!=nil{
            fmt.Println("Error:",err)
        }else{
            defer resp.Body.Close()
            fmt.Println("PUT REQUEST HAS BEEN COMPLETED")
        }  
}  
// GET  KEY METHOD
func GET_KEY(key string,circle *CONSISTENT_HASH){
    var out KEY_VALUE 
    ipAddress:= circle.Get(key)
	address := "http://"+ipAddress.IP+"/keys/"+key
	fmt.Println(address)
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("There is an error :",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}
//  

func GET_ALL(address string){
     
    var out []KEY_VALUE
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

//MAIN START

func main() {   
    circle := NEW_CONSISTENT_HASH()     

    circle.ADD_NEW_NODE(NEW_NODE(0, "127.0.0.1:3000"))

	circle.ADD_NEW_NODE(NEW_NODE(1, "127.0.0.1:3001"))

	circle.ADD_NEW_NODE(NEW_NODE(2, "127.0.0.1:3002")) 
	// DETECT ARGUMENTS
	if(os.Args[1]=="PUT"){
		key := strings.Split(os.Args[2],"/")
        PUT_KEY(circle,key[0],key[1])
    } else if ((os.Args[1]=="GET") && len(os.Args)==3){
    	GET_KEY(os.Args[2],circle)
    } else {
		GET_ALL("http://127.0.0.1:3000/keys")

	    GET_ALL("http://127.0.0.1:3001/keys")
	    GET_ALL("http://127.0.0.1:3002/keys")
	}
} 

// GET LENGTH 
func (hr HASH_CIRC) Len() int {  
    return len(hr)  
}  
// LESSER THAN  
func (hr HASH_CIRC) Less(i, j int) bool {  
    return hr[i] < hr[j]  
}  
//SWAPPING
func (hr HASH_CIRC) Swap(i, j int) {  
    hr[i], hr[j] = hr[j], hr[i]  
}  
