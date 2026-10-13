package message

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestPortableNumbers 检查实际数值而非字面长度；1.0 可读，精度丢失及下溢走字节路径。
func TestPortableNumbers(t *testing.T) {
	for text, want := range map[string]bool{
		`{"v":1.0}`: true, `{"v":10e-1}`: true, `{"v":-0}`: true,
		`{"v":9007199254740992}`: true, `{"v":9007199254740993}`: false,
		`{"v":0.123456789012345678901}`: false, `{"v":1e400}`: false,
		`{"v":1e-400}`: false, `{"v":"1e400"}`: true,
	} {
		if got := Portable([]byte(text)); got != want {
			t.Errorf("%s: portable=%v, want=%v", text, got, want)
		}
	}
}

// TestCanonicalNumbers 对象重排、空白和十进制等价表示不能改变恢复身份。
func TestCanonicalNumbers(t *testing.T) {
	first, err := Canonical([]byte(`{"a":[1,0,-0],"b":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	same, err := Canonical([]byte(`{"b":90071992547409930e-1,"a":[1.0,0e10,0]}`))
	if err != nil || string(first) != string(same) {
		t.Fatal(string(first), string(same), err)
	}
	if _, err := Canonical([]byte(`{} {}`)); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}

// TestProtoArrays 使用标准 well-known 类型证明每项走 ProtoJSON，不依赖生成结构体的 JSON 标签。
func TestProtoArrays(t *testing.T) {
	data, _, err := Marshal([]proto.Message{wrapperspb.Int64(9007199254740993)}, 1024)
	if err != nil || string(data) != `["9007199254740993"]` {
		t.Fatal(string(data), err)
	}
	for _, input := range []any{[]proto.Message{wrapperspb.Int64(1), wrapperspb.String("x")}, []*wrapperspb.Int64Value{nil}, []any{wrapperspb.Int64(1), "x"}} {
		if _, _, err := Marshal(input, 1024); err == nil {
			t.Fatal("invalid protobuf array accepted")
		}
	}
	data, _, err = Marshal([]*wrapperspb.Int64Value{}, 1024)
	if err != nil || string(data) != "[]" {
		t.Fatal(string(data), err)
	}
}

// FuzzCanonical 保证规范化结果仍是有效 JSON，并且再次规范化不改变字节。
func FuzzCanonical(f *testing.F) {
	f.Add([]byte(`{"v":1.0}`))
	f.Add([]byte(`1e100000`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		canonical, err := Canonical(data)
		if err != nil {
			return
		}
		if !json.Valid(canonical) {
			t.Fatal("canonical form is invalid JSON")
		}
		again, err := Canonical(canonical)
		if err != nil || string(again) != string(canonical) {
			t.Fatal("canonical form is unstable", err)
		}
	})
}
