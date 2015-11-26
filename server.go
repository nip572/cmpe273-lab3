package main
import  ("github.com/julienschmidt/httprouter"
    "fmt"
    "net/http"
    "strconv"
    "encoding/json"
    "strings"
    "sort")


type KeyValue struct{
  Key int `json:"key,omitempty"`
  Value string  `json:"value,omitempty"`
} 


var KV1,KV2,KV3 [] KeyValue

var IND1,IND2,IND3 int

type KVArray []KeyValue


func (a KVArray) Len() int           { return len(a) }
func (x KVArray) Swap(i, j int)  { 

  x[i], x[j] = x[j], x[i] 

}
func (a KVArray) Less(i, j int) bool { 

  return a[i].Key < a[j].Key 
}

//GET ALL THE KEYS FOR ALL THE PORTS
func GET_KEYS(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
  port := strings.Split(request.Host,":")
  if(port[1]=="3000"){

    sort.Sort(KVArray(KV1))
    result,_:= json.Marshal(KV1)
    fmt.Fprintln(rw,string(result))

  }else if(port[1]=="3001"){

    sort.Sort(KVArray(KV2))
    result,_:= json.Marshal(KV2)
    fmt.Fprintln(rw,string(result))

  }else{

    sort.Sort(KVArray(KV3))
    result,_:= json.Marshal(KV3)
    fmt.Fprintln(rw,string(result))
  }
}

//PUT KEYS FUNCTION

func PUT_KEYS(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
  port := strings.Split(request.Host,":")
  key,_ := strconv.Atoi(p.ByName("key_id"))
  if(port[1]=="3000"){
    KV1 = append(KV1,KeyValue{key,p.ByName("value")})
    IND1++
  }else if(port[1]=="3001"){
    KV2 = append(KV2,KeyValue{key,p.ByName("value")})
    IND2++
  }else{
    KV3 = append(KV3,KeyValue{key,p.ByName("value")})
    IND3++
  } 
}

//GET SINGLE KEY FUCTION 

func GET_SINGLE_KEY(rw http.ResponseWriter, request *http.Request,p httprouter.Params){ 
  out := KV1

  ind := IND1

  port := strings.Split(request.Host,":")
  if(port[1]=="3001"){     // response for 3001 port

    out = KV2 

    ind = IND2
  }else if(port[1]=="3002"){      // response for 3002 port

    out = KV3

    ind = IND3
  } 
  key,_ := strconv.Atoi(p.ByName("key_id"))
  for i:=0 ; i< ind ;i++{
    if(out[i].Key==key){
      result,_:= json.Marshal(out[i])   //JSON MARSHALLING
      fmt.Fprintln(rw,string(result))
    }
  }
}


// MAIN START

func main(){
  IND1 = 0
  IND2 = 0
  IND3 = 0
  mux := httprouter.New()

    mux.GET("/keys",GET_KEYS)

    mux.GET("/keys/:key_id",GET_SINGLE_KEY)
    
    mux.PUT("/keys/:key_id/:value",PUT_KEYS)

    go http.ListenAndServe(":3000",mux)

    go http.ListenAndServe(":3001",mux)

    go http.ListenAndServe(":3002",mux)

    fmt.Println("Server is Running now...")
    select {}
}