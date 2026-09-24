package breakout

import "testing"

func TestManagerAssignAndRemove(t *testing.T) {
 m:=NewManager(); m.Create("main-b","Room B")
 if !m.Assign("main-b","u1") { t.Fatal("assign failed") }
 if m.Get("main-b").Participants["u1"]!="main-b" { t.Fatal("participant not assigned") }
 if !m.Remove("main-b","u1") { t.Fatal("remove failed") }
 if m.Remove("main-b","u1") { t.Fatal("second remove should fail") }
}
