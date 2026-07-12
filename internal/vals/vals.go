package vals

import "distributed-storage/internal/primitive"

type Value = primitive.Primitive
type ValueType = primitive.PrimitiveType
type Object = primitive.Object
type StringValue = primitive.String

const (
	TYPE_NULL   = primitive.TYPE_NULL
	TYPE_STRING = primitive.TYPE_STRING
	TYPE_INT32  = primitive.TYPE_INT32
	TYPE_INT64  = primitive.TYPE_INT64
	TYPE_UINT32 = primitive.TYPE_UINT32
	TYPE_UINT64 = primitive.TYPE_UINT64
)

func New(t ValueType) Value          { return primitive.New(t) }
func NewString(s string) *StringValue { return primitive.NewString(s) }
func NewUint32(v uint32) *primitive.Uint32 { return primitive.NewUint32(v) }
func NewUint64(v uint64) *primitive.Uint64 { return primitive.NewUint64(v) }
func NewInt32(v int32) *primitive.Int32    { return primitive.NewInt32(v) }
func NewInt64(v int64) *primitive.Int64    { return primitive.NewInt64(v) }
func NewObject() *Object                   { return primitive.NewObject() }
func NewNull() *primitive.Null             { return primitive.NewNull() }
