package vals

import (
	"bytes"
	"testing"
)

// newStringVal builds a StringValue for the given ASCII string (no 0x00/0x01 bytes).
func newStringVal(s string) *StringValue {
	sv := &StringValue{}
	sv.Parse(append([]byte(s), 0x00))
	return sv
}

// ── NullValue ──────────────────────────────────────────────────────────────

func TestNullValue_Type(t *testing.T) {
	if NewNull().Type() != TYPE_NULL {
		t.Errorf("expected TYPE_NULL, got %d", NewNull().Type())
	}
}

func TestNullValue_Empty(t *testing.T) {
	if !NewNull().Empty() {
		t.Error("NullValue.Empty() should return true")
	}
}

func TestNullValue_Equal_AnotherNull(t *testing.T) {
	if !NewNull().Equal(NewNull()) {
		t.Error("two NullValues should be Equal")
	}
}

func TestNullValue_Equal_DifferentType(t *testing.T) {
	if NewNull().Equal(NewInt32(0)) {
		t.Error("NullValue should not Equal a non-null value")
	}
}

func TestNullValue_Serialize_IsEmpty(t *testing.T) {
	if len(NewNull().Serialize()) != 0 {
		t.Error("NullValue.Serialize() should return empty slice")
	}
}

func TestNullValue_Parse_ReturnsZero(t *testing.T) {
	size := NewNull().Parse([]byte{0x01, 0x02, 0x03})
	if size != 0 {
		t.Errorf("NullValue.Parse() should return 0, got %d", size)
	}
}

func TestNewNull_ReturnsNullValue(t *testing.T) {
	v := NewNull()
	if v == nil || v.Type() != TYPE_NULL {
		t.Error("NewNull() should return a valid NullValue")
	}
}

func TestParseNull_ReturnsNullAndZeroSize(t *testing.T) {
	v, size := ParseNull([]byte{0xFF})
	if v == nil || v.Type() != TYPE_NULL {
		t.Error("ParseNull should return NullValue")
	}
	if size != 0 {
		t.Errorf("ParseNull size should be 0, got %d", size)
	}
}

func TestSerializeNull_IsEmpty(t *testing.T) {
	if len(SerializeNull()) != 0 {
		t.Error("SerializeNull() should return empty slice")
	}
}

// ── StringValue ────────────────────────────────────────────────────────────

func TestStringValue_Type(t *testing.T) {
	if newStringVal("x").Type() != TYPE_STRING {
		t.Error("expected TYPE_STRING")
	}
}

func TestStringValue_Empty(t *testing.T) {
	if newStringVal("hello").Empty() {
		t.Error("StringValue.Empty() should return false")
	}
}

func TestStringValue_Value(t *testing.T) {
	if newStringVal("hello").Value() != "hello" {
		t.Error("Value() should return the original string")
	}
}

func TestStringValue_Equal_SameString(t *testing.T) {
	if !newStringVal("abc").Equal(newStringVal("abc")) {
		t.Error("equal strings should be Equal")
	}
}

func TestStringValue_Equal_DifferentString(t *testing.T) {
	if newStringVal("abc").Equal(newStringVal("xyz")) {
		t.Error("different strings should not be Equal")
	}
}

func TestStringValue_Equal_DifferentType(t *testing.T) {
	if newStringVal("1").Equal(NewInt32(1)) {
		t.Error("StringValue should not Equal a non-string value")
	}
}

func TestStringValue_SerializeParse_Roundtrip(t *testing.T) {
	original := newStringVal("hello world")
	serialized := original.Serialize()
	parsed := &StringValue{}
	size := parsed.Parse(serialized)
	if parsed.Value() != "hello world" {
		t.Errorf("roundtrip failed: got %q", parsed.Value())
	}
	if size != len(serialized) {
		t.Errorf("Parse size mismatch: got %d, want %d", size, len(serialized))
	}
}

func TestStringValue_Serialize_NullTerminated(t *testing.T) {
	b := newStringVal("hi").Serialize()
	if b[len(b)-1] != 0x00 {
		t.Error("serialized string should end with 0x00 terminator")
	}
}

func TestStringValue_Serialize_EscapesNullByte(t *testing.T) {
	// A 0x00 byte in the string must be escaped as 0x01 0x01.
	sv := &StringValue{str: []byte{0x00}}
	b := sv.Serialize()
	if !bytes.HasPrefix(b, []byte{0x01, 0x01}) {
		t.Errorf("0x00 byte should serialize as 0x01 0x01 prefix, got %v", b)
	}
}

func TestStringValue_Serialize_EscapesOneByte(t *testing.T) {
	// A 0x01 byte in the string must be escaped as 0x01 0x02.
	sv := &StringValue{str: []byte{0x01}}
	b := sv.Serialize()
	if !bytes.HasPrefix(b, []byte{0x01, 0x02}) {
		t.Errorf("0x01 byte should serialize as 0x01 0x02 prefix, got %v", b)
	}
}

func TestStringValue_Parse_EmptyString(t *testing.T) {
	sv := &StringValue{}
	size := sv.Parse([]byte{0x00}) // just the null terminator
	if sv.Value() != "" {
		t.Errorf("expected empty string, got %q", sv.Value())
	}
	if size != 1 {
		t.Errorf("expected size 1, got %d", size)
	}
}

// ── Int32Value ─────────────────────────────────────────────────────────────

func TestInt32Value_Type(t *testing.T) {
	if NewInt32(0).Type() != TYPE_INT32 {
		t.Error("expected TYPE_INT32")
	}
}

func TestInt32Value_Empty(t *testing.T) {
	if NewInt32(0).Empty() {
		t.Error("Int32Value.Empty() should return false")
	}
}

func TestInt32Value_Value(t *testing.T) {
	if NewInt32(42).Value() != 42 {
		t.Error("Value() should return 42")
	}
}

func TestInt32Value_Equal_SameValue(t *testing.T) {
	if !NewInt32(5).Equal(NewInt32(5)) {
		t.Error("same Int32 values should be Equal")
	}
}

func TestInt32Value_Equal_DifferentValue(t *testing.T) {
	if NewInt32(1).Equal(NewInt32(2)) {
		t.Error("different Int32 values should not be Equal")
	}
}

func TestInt32Value_Equal_DifferentType(t *testing.T) {
	if NewInt32(1).Equal(NewInt64(1)) {
		t.Error("Int32 should not Equal Int64")
	}
}

func TestInt32Value_SerializeParse_Positive(t *testing.T) {
	parsed, size := ParseInt32(NewInt32(100).Serialize())
	if parsed.Value() != 100 || size != 4 {
		t.Errorf("got %d, size %d", parsed.Value(), size)
	}
}

func TestInt32Value_SerializeParse_Negative(t *testing.T) {
	parsed, size := ParseInt32(NewInt32(-100).Serialize())
	if parsed.Value() != -100 || size != 4 {
		t.Errorf("got %d, size %d", parsed.Value(), size)
	}
}

func TestInt32Value_SerializeParse_Zero(t *testing.T) {
	parsed, _ := ParseInt32(NewInt32(0).Serialize())
	if parsed.Value() != 0 {
		t.Errorf("expected 0, got %d", parsed.Value())
	}
}

func TestInt32Value_SerializeParse_MinValue(t *testing.T) {
	const minInt32 = int32(-2147483648)
	parsed, _ := ParseInt32(NewInt32(minInt32).Serialize())
	if parsed.Value() != minInt32 {
		t.Errorf("expected %d, got %d", minInt32, parsed.Value())
	}
}

func TestInt32Value_SerializeParse_MaxValue(t *testing.T) {
	const maxInt32 = int32(2147483647)
	parsed, _ := ParseInt32(NewInt32(maxInt32).Serialize())
	if parsed.Value() != maxInt32 {
		t.Errorf("expected %d, got %d", maxInt32, parsed.Value())
	}
}

func TestInt32Value_Serialize_Size(t *testing.T) {
	if len(NewInt32(0).Serialize()) != 4 {
		t.Error("Int32 serialized size should be 4")
	}
}

func TestNewInt32_ReturnsCorrectValue(t *testing.T) {
	v := NewInt32(-7)
	if v.Value() != -7 {
		t.Errorf("NewInt32(-7).Value() = %d", v.Value())
	}
}

func TestSerializeInt32_Roundtrip(t *testing.T) {
	parsed, _ := ParseInt32(SerializeInt32(7))
	if parsed.Value() != 7 {
		t.Errorf("SerializeInt32 roundtrip failed: got %d", parsed.Value())
	}
}

// ── Int64Value ─────────────────────────────────────────────────────────────

func TestInt64Value_Type(t *testing.T) {
	if NewInt64(0).Type() != TYPE_INT64 {
		t.Error("expected TYPE_INT64")
	}
}

func TestInt64Value_Empty(t *testing.T) {
	if NewInt64(0).Empty() {
		t.Error("Int64Value.Empty() should return false")
	}
}

func TestInt64Value_Value(t *testing.T) {
	if NewInt64(999).Value() != 999 {
		t.Error("Value() should return 999")
	}
}

func TestInt64Value_Equal_SameValue(t *testing.T) {
	if !NewInt64(10).Equal(NewInt64(10)) {
		t.Error("same Int64 values should be Equal")
	}
}

func TestInt64Value_Equal_DifferentValue(t *testing.T) {
	if NewInt64(1).Equal(NewInt64(2)) {
		t.Error("different Int64 values should not be Equal")
	}
}

func TestInt64Value_Equal_DifferentType(t *testing.T) {
	if NewInt64(1).Equal(NewInt32(1)) {
		t.Error("Int64 should not Equal Int32")
	}
}

func TestInt64Value_SerializeParse_Positive(t *testing.T) {
	parsed, size := ParseInt64(NewInt64(12345678).Serialize())
	if parsed.Value() != 12345678 || size != 8 {
		t.Errorf("got %d, size %d", parsed.Value(), size)
	}
}

func TestInt64Value_SerializeParse_Negative(t *testing.T) {
	parsed, size := ParseInt64(NewInt64(-99999).Serialize())
	if parsed.Value() != -99999 || size != 8 {
		t.Errorf("got %d, size %d", parsed.Value(), size)
	}
}

func TestInt64Value_SerializeParse_Zero(t *testing.T) {
	parsed, _ := ParseInt64(NewInt64(0).Serialize())
	if parsed.Value() != 0 {
		t.Errorf("expected 0, got %d", parsed.Value())
	}
}

func TestInt64Value_Serialize_Size(t *testing.T) {
	if len(NewInt64(0).Serialize()) != 8 {
		t.Error("Int64 serialized size should be 8")
	}
}

func TestNewInt64_ReturnsCorrectValue(t *testing.T) {
	v := NewInt64(-9999999999)
	if v.Value() != -9999999999 {
		t.Errorf("NewInt64(-9999999999).Value() = %d", v.Value())
	}
}

func TestSerializeInt64_Roundtrip(t *testing.T) {
	parsed, _ := ParseInt64(SerializeInt64(-42))
	if parsed.Value() != -42 {
		t.Errorf("SerializeInt64 roundtrip failed: got %d", parsed.Value())
	}
}

// ── Uint32Value ────────────────────────────────────────────────────────────

func TestUint32Value_Type(t *testing.T) {
	if NewUint32(0).Type() != TYPE_UINT32 {
		t.Error("expected TYPE_UINT32")
	}
}

func TestUint32Value_Empty(t *testing.T) {
	if NewUint32(0).Empty() {
		t.Error("Uint32Value.Empty() should return false")
	}
}

func TestUint32Value_Value(t *testing.T) {
	if NewUint32(42).Value() != 42 {
		t.Error("Value() should return 42")
	}
}

func TestUint32Value_Equal_SameValue(t *testing.T) {
	if !NewUint32(5).Equal(NewUint32(5)) {
		t.Error("same Uint32 values should be Equal")
	}
}

func TestUint32Value_Equal_DifferentValue(t *testing.T) {
	if NewUint32(1).Equal(NewUint32(2)) {
		t.Error("different Uint32 values should not be Equal")
	}
}

func TestUint32Value_Equal_DifferentType(t *testing.T) {
	if NewUint32(1).Equal(NewUint64(1)) {
		t.Error("Uint32 should not Equal Uint64")
	}
}

func TestUint32Value_SerializeParse_Roundtrip(t *testing.T) {
	parsed, size := ParseUint32(NewUint32(0xDEADBEEF).Serialize())
	if parsed.Value() != 0xDEADBEEF || size != 4 {
		t.Errorf("got %d, size %d", parsed.Value(), size)
	}
}

func TestUint32Value_SerializeParse_Zero(t *testing.T) {
	parsed, _ := ParseUint32(NewUint32(0).Serialize())
	if parsed.Value() != 0 {
		t.Errorf("expected 0, got %d", parsed.Value())
	}
}

func TestUint32Value_Serialize_Size(t *testing.T) {
	if len(NewUint32(0).Serialize()) != 4 {
		t.Error("Uint32 serialized size should be 4")
	}
}

func TestNewUint32_ReturnsCorrectValue(t *testing.T) {
	v := NewUint32(100)
	if v.Value() != 100 {
		t.Errorf("NewUint32(100).Value() = %d", v.Value())
	}
}

func TestSerializeUint32_Roundtrip(t *testing.T) {
	parsed, _ := ParseUint32(SerializeUint32(999))
	if parsed.Value() != 999 {
		t.Errorf("SerializeUint32 roundtrip failed: got %d", parsed.Value())
	}
}

// ── Uint64Value ────────────────────────────────────────────────────────────

func TestUint64Value_Type(t *testing.T) {
	if NewUint64(0).Type() != TYPE_UINT64 {
		t.Error("expected TYPE_UINT64")
	}
}

func TestUint64Value_Empty(t *testing.T) {
	if NewUint64(0).Empty() {
		t.Error("Uint64Value.Empty() should return false")
	}
}

func TestUint64Value_Value(t *testing.T) {
	if NewUint64(1<<40).Value() != 1<<40 {
		t.Error("Value() should return 1<<40")
	}
}

func TestUint64Value_Equal_SameValue(t *testing.T) {
	if !NewUint64(100).Equal(NewUint64(100)) {
		t.Error("same Uint64 values should be Equal")
	}
}

func TestUint64Value_Equal_DifferentValue(t *testing.T) {
	if NewUint64(1).Equal(NewUint64(2)) {
		t.Error("different Uint64 values should not be Equal")
	}
}

func TestUint64Value_Equal_DifferentType(t *testing.T) {
	if NewUint64(1).Equal(NewUint32(1)) {
		t.Error("Uint64 should not Equal Uint32")
	}
}

func TestUint64Value_SerializeParse_Roundtrip(t *testing.T) {
	const val = uint64(0xDEADBEEFCAFEBABE)
	parsed, size := ParseUint64(NewUint64(val).Serialize())
	if parsed.Value() != val || size != 8 {
		t.Errorf("got %d, size %d", parsed.Value(), size)
	}
}

func TestUint64Value_SerializeParse_Zero(t *testing.T) {
	parsed, _ := ParseUint64(NewUint64(0).Serialize())
	if parsed.Value() != 0 {
		t.Errorf("expected 0, got %d", parsed.Value())
	}
}

func TestUint64Value_Serialize_Size(t *testing.T) {
	if len(NewUint64(0).Serialize()) != 8 {
		t.Error("Uint64 serialized size should be 8")
	}
}

func TestNewUint64_ReturnsCorrectValue(t *testing.T) {
	v := NewUint64(1234567890123)
	if v.Value() != 1234567890123 {
		t.Errorf("NewUint64 returned %d", v.Value())
	}
}

func TestSerializeUint64_Roundtrip(t *testing.T) {
	parsed, _ := ParseUint64(SerializeUint64(1234567890123))
	if parsed.Value() != 1234567890123 {
		t.Errorf("SerializeUint64 roundtrip failed: got %d", parsed.Value())
	}
}

// ── New / ParseValue ───────────────────────────────────────────────────────

func TestNew_Null(t *testing.T) {
	if New(TYPE_NULL).Type() != TYPE_NULL {
		t.Error("New(TYPE_NULL) should return NullValue")
	}
}

func TestNew_String(t *testing.T) {
	if New(TYPE_STRING).Type() != TYPE_STRING {
		t.Error("New(TYPE_STRING) should return StringValue")
	}
}

func TestNew_Int32(t *testing.T) {
	if New(TYPE_INT32).Type() != TYPE_INT32 {
		t.Error("New(TYPE_INT32) should return Int32Value")
	}
}

func TestNew_Int64(t *testing.T) {
	if New(TYPE_INT64).Type() != TYPE_INT64 {
		t.Error("New(TYPE_INT64) should return Int64Value")
	}
}

func TestNew_Uint32(t *testing.T) {
	if New(TYPE_UINT32).Type() != TYPE_UINT32 {
		t.Error("New(TYPE_UINT32) should return Uint32Value")
	}
}

func TestNew_Uint64(t *testing.T) {
	if New(TYPE_UINT64).Type() != TYPE_UINT64 {
		t.Error("New(TYPE_UINT64) should return Uint64Value")
	}
}

func TestNew_UnknownType_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("New with unknown type should panic")
		}
	}()
	New(ValueType(99))
}

func TestParseValue_Null(t *testing.T) {
	v, size := ParseValue(TYPE_NULL, []byte{})
	if v.Type() != TYPE_NULL || size != 0 {
		t.Errorf("ParseValue(NULL): type=%d size=%d", v.Type(), size)
	}
}

func TestParseValue_Int32(t *testing.T) {
	v, size := ParseValue(TYPE_INT32, SerializeInt32(77))
	if v.(*Int32Value).Value() != 77 || size != 4 {
		t.Errorf("ParseValue(INT32): value=%d size=%d", v.(*Int32Value).Value(), size)
	}
}

func TestParseValue_Uint64(t *testing.T) {
	v, size := ParseValue(TYPE_UINT64, SerializeUint64(888))
	if v.(*Uint64Value).Value() != 888 || size != 8 {
		t.Errorf("ParseValue(UINT64): value=%d size=%d", v.(*Uint64Value).Value(), size)
	}
}

// ── Object ─────────────────────────────────────────────────────────────────

func TestNewObject_Empty(t *testing.T) {
	obj := NewObject()
	if obj == nil {
		t.Fatal("NewObject() should not return nil")
	}
	if len(obj.Values()) != 0 {
		t.Error("new object should have no fields")
	}
}

func TestObject_Set_Get(t *testing.T) {
	obj := NewObject()
	obj.Set("key", NewInt32(42))
	if obj.Get("key").(*Int32Value).Value() != 42 {
		t.Error("Get should return the value that was Set")
	}
}

func TestObject_Get_MissingField_ReturnsNull(t *testing.T) {
	if NewObject().Get("missing").Type() != TYPE_NULL {
		t.Error("missing field should return NullValue")
	}
}

func TestObject_Has_Present(t *testing.T) {
	obj := NewObject()
	obj.Set("x", NewInt32(1))
	if !obj.Has("x") {
		t.Error("Has() should return true for a present field")
	}
}

func TestObject_Has_Absent(t *testing.T) {
	if NewObject().Has("missing") {
		t.Error("Has() should return false for an absent field")
	}
}

func TestObject_GetMany(t *testing.T) {
	obj := NewObject()
	obj.Set("a", NewInt32(1))
	obj.Set("b", NewInt32(2))
	got := obj.GetMany([]string{"a", "b"})
	if len(got) != 2 {
		t.Fatalf("expected 2 values, got %d", len(got))
	}
	if got[0].(*Int32Value).Value() != 1 {
		t.Error("GetMany()[0] should be 1")
	}
	if got[1].(*Int32Value).Value() != 2 {
		t.Error("GetMany()[1] should be 2")
	}
}

func TestObject_GetString(t *testing.T) {
	obj := NewObject()
	obj.Set("s", newStringVal("hello"))
	if obj.GetString("s") != "hello" {
		t.Error("GetString should return the stored string")
	}
}

func TestObject_GetUint32(t *testing.T) {
	obj := NewObject()
	obj.Set("n", NewUint32(7))
	if obj.GetUint32("n") != 7 {
		t.Error("GetUint32 should return 7")
	}
}

func TestObject_GetUint64(t *testing.T) {
	obj := NewObject()
	obj.Set("n", NewUint64(999999))
	if obj.GetUint64("n") != 999999 {
		t.Error("GetUint64 should return 999999")
	}
}

func TestObject_GetInt32(t *testing.T) {
	obj := NewObject()
	obj.Set("n", NewInt32(-5))
	if obj.GetInt32("n") != -5 {
		t.Error("GetInt32 should return -5")
	}
}

func TestObject_GetInt64(t *testing.T) {
	obj := NewObject()
	obj.Set("n", NewInt64(-9999999999))
	if obj.GetInt64("n") != -9999999999 {
		t.Error("GetInt64 should return -9999999999")
	}
}

func TestObject_Values_ReturnsAllFields(t *testing.T) {
	obj := NewObject()
	obj.Set("a", NewInt32(1))
	obj.Set("b", NewInt32(2))
	if len(obj.Values()) != 2 {
		t.Errorf("expected 2 fields, got %d", len(obj.Values()))
	}
}

func TestObject_Matches_Superset(t *testing.T) {
	// obj1 has one field; obj2 is a superset — obj1.Matches(obj2) should be true.
	obj1 := NewObject().Set("x", NewInt32(1))
	obj2 := NewObject().Set("x", NewInt32(1)).Set("y", NewInt32(2))
	if !obj1.Matches(obj2) {
		t.Error("obj1 should match obj2 when obj2 is a superset with equal values")
	}
}

func TestObject_Matches_WrongValue(t *testing.T) {
	obj1 := NewObject().Set("x", NewInt32(1))
	obj2 := NewObject().Set("x", NewInt32(99))
	if obj1.Matches(obj2) {
		t.Error("objects with different values should not match")
	}
}

func TestObject_Matches_NilOther(t *testing.T) {
	obj := NewObject().Set("x", NewInt32(1))
	if obj.Matches(nil) {
		t.Error("Matches(nil) should return false")
	}
}

func TestObject_Merge_TwoObjects(t *testing.T) {
	a := NewObject().Set("x", NewInt32(1))
	b := NewObject().Set("y", NewInt32(2))
	merged := a.Merge(b)
	if !merged.Has("x") || !merged.Has("y") {
		t.Error("merged object should have fields from both objects")
	}
}

func TestObject_Merge_NilOther(t *testing.T) {
	a := NewObject().Set("x", NewInt32(1))
	merged := a.Merge(nil)
	if !merged.Has("x") {
		t.Error("merged object should retain fields when other is nil")
	}
}

func TestObject_Merge_SecondOverridesFirst(t *testing.T) {
	a := NewObject().Set("x", NewInt32(1))
	b := NewObject().Set("x", NewInt32(99))
	merged := a.Merge(b)
	if merged.GetInt32("x") != 99 {
		t.Errorf("expected merged x=99, got %d", merged.GetInt32("x"))
	}
}

func TestObject_Merge_DoesNotMutateOriginal(t *testing.T) {
	a := NewObject().Set("x", NewInt32(1))
	b := NewObject().Set("x", NewInt32(99))
	_ = a.Merge(b)
	if a.GetInt32("x") != 1 {
		t.Error("Merge should not mutate the original object")
	}
}
