package main

import (
 "crypto/hmac"
 "crypto/sha256"
 "encoding/base64"
 "encoding/json"
 "flag"
 "fmt"
 "net/http"
 "sync"
 "time"
 "github.com/gorilla/websocket"
)

type claims struct { RoomID, UserID, Role string; Exp int64 }

func token(room,user,role,secret string) string {
 p,_:=json.Marshal(claims{room,user,role,time.Now().Unix()+3600})
 b:=base64.RawURLEncoding.EncodeToString(p)
 h:=hmac.New(sha256.New,[]byte(secret)); _,_=h.Write([]byte(b))
 return b+"."+base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}
func main(){
 n:=flag.Int("clients",10,"concurrent clients"); room:=flag.String("room","LOAD01","room id"); url:=flag.String("url","ws://127.0.0.1:4000/ws","SFU websocket URL"); secret:=flag.String("secret","","ROOM_ACCESS_SECRET"); flag.Parse()
 if *secret=="" { panic("set -secret") }
 var wg sync.WaitGroup; var ok,fail int; var mu sync.Mutex
 for i:=0;i<*n;i++ { wg.Add(1); go func(i int){ defer wg.Done()
   c,_,err:=websocket.DefaultDialer.Dial(*url,http.Header{})
   if err!=nil { mu.Lock(); fail++; mu.Unlock(); return }
   defer c.Close()
   msg:=map[string]any{"type":"join","roomId":*room,"data":map[string]string{"accessToken":token(*room,fmt.Sprintf("load-%d",i),"member",*secret)}}
   if err=c.WriteJSON(msg); err!=nil { mu.Lock(); fail++; mu.Unlock(); return }
   c.SetReadDeadline(time.Now().Add(5*time.Second)); if _,_,err=c.ReadMessage(); err!=nil { mu.Lock(); fail++; mu.Unlock(); return }
   mu.Lock(); ok++; mu.Unlock()
 }(i)}
 wg.Wait(); fmt.Printf("clients=%d ok=%d failed=%d\n",*n,ok,fail)
 if fail>0 { panic("load test failures") }
}
