// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: tempo.proto

package tempopb

import (
	context "context"
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	v1 "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type TraceByIDRequest struct {
	TraceID        []byte `protobuf:"bytes,1,opt,name=traceID,proto3" json:"traceID,omitempty"`
	BlockStart     string `protobuf:"bytes,2,opt,name=blockStart,proto3" json:"blockStart,omitempty"`
	BlockEnd       string `protobuf:"bytes,3,opt,name=blockEnd,proto3" json:"blockEnd,omitempty"`
	QueryIngesters bool   `protobuf:"varint,4,opt,name=queryIngesters,proto3" json:"queryIngesters,omitempty"`
}

func (m *TraceByIDRequest) Reset()         { *m = TraceByIDRequest{} }
func (m *TraceByIDRequest) String() string { return proto.CompactTextString(m) }
func (*TraceByIDRequest) ProtoMessage()    {}
func (*TraceByIDRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_b334b194b16825ec, []int{0}
}
func (m *TraceByIDRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TraceByIDRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TraceByIDRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TraceByIDRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TraceByIDRequest.Merge(m, src)
}
func (m *TraceByIDRequest) XXX_Size() int {
	return m.Size()
}
func (m *TraceByIDRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_TraceByIDRequest.DiscardUnknown(m)
}

var xxx_messageInfo_TraceByIDRequest proto.InternalMessageInfo

func (m *TraceByIDRequest) GetTraceID() []byte {
	if m != nil {
		return m.TraceID
	}
	return nil
}

func (m *TraceByIDRequest) GetBlockStart() string {
	if m != nil {
		return m.BlockStart
	}
	return ""
}

func (m *TraceByIDRequest) GetBlockEnd() string {
	if m != nil {
		return m.BlockEnd
	}
	return ""
}

func (m *TraceByIDRequest) GetQueryIngesters() bool {
	if m != nil {
		return m.QueryIngesters
	}
	return false
}

type TraceByIDResponse struct {
	Trace *Trace `protobuf:"bytes,1,opt,name=trace,proto3" json:"trace,omitempty"`
}

func (m *TraceByIDResponse) Reset()         { *m = TraceByIDResponse{} }
func (m *TraceByIDResponse) String() string { return proto.CompactTextString(m) }
func (*TraceByIDResponse) ProtoMessage()    {}
func (*TraceByIDResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_b334b194b16825ec, []int{1}
}
func (m *TraceByIDResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TraceByIDResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TraceByIDResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TraceByIDResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TraceByIDResponse.Merge(m, src)
}
func (m *TraceByIDResponse) XXX_Size() int {
	return m.Size()
}
func (m *TraceByIDResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_TraceByIDResponse.DiscardUnknown(m)
}

var xxx_messageInfo_TraceByIDResponse proto.InternalMessageInfo

func (m *TraceByIDResponse) GetTrace() *Trace {
	if m != nil {
		return m.Trace
	}
	return nil
}

type Trace struct {
	Batches []*v1.ResourceSpans `protobuf:"bytes,1,rep,name=batches,proto3" json:"batches,omitempty"`
}

func (m *Trace) Reset()         { *m = Trace{} }
func (m *Trace) String() string { return proto.CompactTextString(m) }
func (*Trace) ProtoMessage()    {}
func (*Trace) Descriptor() ([]byte, []int) {
	return fileDescriptor_b334b194b16825ec, []int{2}
}
func (m *Trace) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Trace) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Trace.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Trace) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Trace.Merge(m, src)
}
func (m *Trace) XXX_Size() int {
	return m.Size()
}
func (m *Trace) XXX_DiscardUnknown() {
	xxx_messageInfo_Trace.DiscardUnknown(m)
}

var xxx_messageInfo_Trace proto.InternalMessageInfo

func (m *Trace) GetBatches() []*v1.ResourceSpans {
	if m != nil {
		return m.Batches
	}
	return nil
}

type PushRequest struct {
	Batch *v1.ResourceSpans `protobuf:"bytes,1,opt,name=batch,proto3" json:"batch,omitempty"`
}

func (m *PushRequest) Reset()         { *m = PushRequest{} }
func (m *PushRequest) String() string { return proto.CompactTextString(m) }
func (*PushRequest) ProtoMessage()    {}
func (*PushRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_b334b194b16825ec, []int{3}
}
func (m *PushRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PushRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PushRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PushRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PushRequest.Merge(m, src)
}
func (m *PushRequest) XXX_Size() int {
	return m.Size()
}
func (m *PushRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PushRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PushRequest proto.InternalMessageInfo

func (m *PushRequest) GetBatch() *v1.ResourceSpans {
	if m != nil {
		return m.Batch
	}
	return nil
}

type PushResponse struct {
}

func (m *PushResponse) Reset()         { *m = PushResponse{} }
func (m *PushResponse) String() string { return proto.CompactTextString(m) }
func (*PushResponse) ProtoMessage()    {}
func (*PushResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_b334b194b16825ec, []int{4}
}
func (m *PushResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PushResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PushResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PushResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PushResponse.Merge(m, src)
}
func (m *PushResponse) XXX_Size() int {
	return m.Size()
}
func (m *PushResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_PushResponse.DiscardUnknown(m)
}

var xxx_messageInfo_PushResponse proto.InternalMessageInfo

type PushBytesRequest struct {
	// pre-serialized PushRequests
	Requests [][]byte `protobuf:"bytes,1,rep,name=requests,proto3" json:"requests,omitempty"`
}

func (m *PushBytesRequest) Reset()         { *m = PushBytesRequest{} }
func (m *PushBytesRequest) String() string { return proto.CompactTextString(m) }
func (*PushBytesRequest) ProtoMessage()    {}
func (*PushBytesRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_b334b194b16825ec, []int{5}
}
func (m *PushBytesRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PushBytesRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PushBytesRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PushBytesRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PushBytesRequest.Merge(m, src)
}
func (m *PushBytesRequest) XXX_Size() int {
	return m.Size()
}
func (m *PushBytesRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PushBytesRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PushBytesRequest proto.InternalMessageInfo

func (m *PushBytesRequest) GetRequests() [][]byte {
	if m != nil {
		return m.Requests
	}
	return nil
}

func init() {
	proto.RegisterType((*TraceByIDRequest)(nil), "tempopb.TraceByIDRequest")
	proto.RegisterType((*TraceByIDResponse)(nil), "tempopb.TraceByIDResponse")
	proto.RegisterType((*Trace)(nil), "tempopb.Trace")
	proto.RegisterType((*PushRequest)(nil), "tempopb.PushRequest")
	proto.RegisterType((*PushResponse)(nil), "tempopb.PushResponse")
	proto.RegisterType((*PushBytesRequest)(nil), "tempopb.PushBytesRequest")
}

func init() { proto.RegisterFile("tempo.proto", fileDescriptor_b334b194b16825ec) }

var fileDescriptor_b334b194b16825ec = []byte{
	// 406 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x52, 0x41, 0x8b, 0xd3, 0x40,
	0x14, 0xce, 0xb8, 0xdb, 0x4d, 0xf7, 0xa5, 0x96, 0x75, 0x50, 0x88, 0x39, 0x84, 0x10, 0x44, 0x02,
	0x42, 0xca, 0x46, 0x3c, 0x78, 0x12, 0xcb, 0xae, 0xd8, 0x8b, 0xac, 0x53, 0xff, 0x40, 0x92, 0x3e,
	0x6c, 0xb1, 0x4d, 0xd2, 0x99, 0x49, 0x21, 0x37, 0x7f, 0x42, 0x7f, 0x96, 0xc7, 0x1e, 0x3d, 0x4a,
	0xfb, 0x47, 0x24, 0x33, 0x4d, 0x48, 0x8b, 0x1e, 0xf6, 0x36, 0xdf, 0xfb, 0xde, 0xfb, 0xde, 0xf7,
	0xbe, 0x04, 0x2c, 0x89, 0xab, 0x22, 0x0f, 0x0b, 0x9e, 0xcb, 0x9c, 0x9a, 0x0a, 0x14, 0x89, 0x13,
	0xe4, 0x05, 0x66, 0x12, 0x97, 0xb8, 0x42, 0xc9, 0xab, 0x91, 0x62, 0x47, 0x92, 0xc7, 0x29, 0x8e,
	0x36, 0xb7, 0xfa, 0xa1, 0x47, 0xfc, 0x2d, 0x81, 0x9b, 0x6f, 0x35, 0x1e, 0x57, 0x93, 0x3b, 0x86,
	0xeb, 0x12, 0x85, 0xa4, 0x36, 0x98, 0xaa, 0x67, 0x72, 0x67, 0x13, 0x8f, 0x04, 0x03, 0xd6, 0x40,
	0xea, 0x02, 0x24, 0xcb, 0x3c, 0xfd, 0x31, 0x95, 0x31, 0x97, 0xf6, 0x13, 0x8f, 0x04, 0xd7, 0xac,
	0x53, 0xa1, 0x0e, 0xf4, 0x15, 0xba, 0xcf, 0x66, 0xf6, 0x85, 0x62, 0x5b, 0x4c, 0x5f, 0xc3, 0x70,
	0x5d, 0x22, 0xaf, 0x26, 0xd9, 0x77, 0x14, 0x12, 0xb9, 0xb0, 0x2f, 0x3d, 0x12, 0xf4, 0xd9, 0x59,
	0xd5, 0x7f, 0x0f, 0xcf, 0x3a, 0x8e, 0x44, 0x91, 0x67, 0x02, 0xe9, 0x2b, 0xe8, 0x29, 0x0f, 0xca,
	0x90, 0x15, 0x0d, 0xc3, 0xe3, 0xa9, 0xa1, 0x6a, 0x65, 0x9a, 0xf4, 0xbf, 0x40, 0x4f, 0x61, 0x7a,
	0x0f, 0x66, 0x12, 0xcb, 0x74, 0x8e, 0xc2, 0x26, 0xde, 0x45, 0x60, 0x45, 0x6f, 0xc2, 0x93, 0x48,
	0xf4, 0xf5, 0xa1, 0x4e, 0x62, 0x73, 0x1b, 0x32, 0x14, 0x79, 0xc9, 0x53, 0x9c, 0x16, 0x71, 0x26,
	0x58, 0x33, 0xeb, 0x3f, 0x80, 0xf5, 0x50, 0x8a, 0x79, 0x93, 0xcb, 0x47, 0xe8, 0x29, 0xe6, 0x68,
	0xe2, 0x51, 0x9a, 0x7a, 0xd2, 0x1f, 0xc2, 0x40, 0x2b, 0xea, 0xbb, 0xfc, 0x10, 0x6e, 0x6a, 0x3c,
	0xae, 0x24, 0x8a, 0x66, 0x8d, 0x03, 0x7d, 0xae, 0x9f, 0xda, 0xfd, 0x80, 0xb5, 0x38, 0xfa, 0x49,
	0xe0, 0xaa, 0x1e, 0x40, 0x4e, 0xdf, 0xc1, 0x65, 0xfd, 0xa2, 0xcf, 0xdb, 0x2c, 0x3a, 0x5e, 0x9d,
	0x17, 0x67, 0xd5, 0xe3, 0x3e, 0x83, 0x7e, 0x80, 0xeb, 0x76, 0x23, 0x7d, 0x79, 0xd2, 0xd5, 0x75,
	0xf1, 0x5f, 0x81, 0x68, 0x0a, 0xe6, 0xd7, 0x12, 0xf9, 0x02, 0x39, 0xfd, 0x0c, 0x4f, 0x3f, 0x2d,
	0xb2, 0x59, 0xfb, 0xb9, 0x3a, 0x7a, 0xe7, 0x3f, 0x95, 0xe3, 0xfc, 0x8b, 0x6a, 0x44, 0xc7, 0xf6,
	0xaf, 0xbd, 0x4b, 0x76, 0x7b, 0x97, 0xfc, 0xd9, 0xbb, 0x64, 0x7b, 0x70, 0x8d, 0xdd, 0xc1, 0x35,
	0x7e, 0x1f, 0x5c, 0x23, 0xb9, 0x52, 0xb1, 0xbe, 0xfd, 0x1b, 0x00, 0x00, 0xff, 0xff, 0xdc, 0xaf,
	0xc5, 0x94, 0xea, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// PusherClient is the client API for Pusher service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type PusherClient interface {
	Push(ctx context.Context, in *PushRequest, opts ...grpc.CallOption) (*PushResponse, error)
	PushBytes(ctx context.Context, in *PushBytesRequest, opts ...grpc.CallOption) (*PushResponse, error)
}

type pusherClient struct {
	cc *grpc.ClientConn
}

func NewPusherClient(cc *grpc.ClientConn) PusherClient {
	return &pusherClient{cc}
}

func (c *pusherClient) Push(ctx context.Context, in *PushRequest, opts ...grpc.CallOption) (*PushResponse, error) {
	out := new(PushResponse)
	err := c.cc.Invoke(ctx, "/tempopb.Pusher/Push", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pusherClient) PushBytes(ctx context.Context, in *PushBytesRequest, opts ...grpc.CallOption) (*PushResponse, error) {
	out := new(PushResponse)
	err := c.cc.Invoke(ctx, "/tempopb.Pusher/PushBytes", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PusherServer is the server API for Pusher service.
type PusherServer interface {
	Push(context.Context, *PushRequest) (*PushResponse, error)
	PushBytes(context.Context, *PushBytesRequest) (*PushResponse, error)
}

// UnimplementedPusherServer can be embedded to have forward compatible implementations.
type UnimplementedPusherServer struct {
}

func (*UnimplementedPusherServer) Push(ctx context.Context, req *PushRequest) (*PushResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Push not implemented")
}
func (*UnimplementedPusherServer) PushBytes(ctx context.Context, req *PushBytesRequest) (*PushResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PushBytes not implemented")
}

func RegisterPusherServer(s *grpc.Server, srv PusherServer) {
	s.RegisterService(&_Pusher_serviceDesc, srv)
}

func _Pusher_Push_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PushRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PusherServer).Push(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tempopb.Pusher/Push",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PusherServer).Push(ctx, req.(*PushRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Pusher_PushBytes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PushBytesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PusherServer).PushBytes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tempopb.Pusher/PushBytes",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PusherServer).PushBytes(ctx, req.(*PushBytesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Pusher_serviceDesc = grpc.ServiceDesc{
	ServiceName: "tempopb.Pusher",
	HandlerType: (*PusherServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Push",
			Handler:    _Pusher_Push_Handler,
		},
		{
			MethodName: "PushBytes",
			Handler:    _Pusher_PushBytes_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "tempo.proto",
}

// QuerierClient is the client API for Querier service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type QuerierClient interface {
	FindTraceByID(ctx context.Context, in *TraceByIDRequest, opts ...grpc.CallOption) (*TraceByIDResponse, error)
}

type querierClient struct {
	cc *grpc.ClientConn
}

func NewQuerierClient(cc *grpc.ClientConn) QuerierClient {
	return &querierClient{cc}
}

func (c *querierClient) FindTraceByID(ctx context.Context, in *TraceByIDRequest, opts ...grpc.CallOption) (*TraceByIDResponse, error) {
	out := new(TraceByIDResponse)
	err := c.cc.Invoke(ctx, "/tempopb.Querier/FindTraceByID", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// QuerierServer is the server API for Querier service.
type QuerierServer interface {
	FindTraceByID(context.Context, *TraceByIDRequest) (*TraceByIDResponse, error)
}

// UnimplementedQuerierServer can be embedded to have forward compatible implementations.
type UnimplementedQuerierServer struct {
}

func (*UnimplementedQuerierServer) FindTraceByID(ctx context.Context, req *TraceByIDRequest) (*TraceByIDResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FindTraceByID not implemented")
}

func RegisterQuerierServer(s *grpc.Server, srv QuerierServer) {
	s.RegisterService(&_Querier_serviceDesc, srv)
}

func _Querier_FindTraceByID_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TraceByIDRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QuerierServer).FindTraceByID(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tempopb.Querier/FindTraceByID",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QuerierServer).FindTraceByID(ctx, req.(*TraceByIDRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Querier_serviceDesc = grpc.ServiceDesc{
	ServiceName: "tempopb.Querier",
	HandlerType: (*QuerierServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FindTraceByID",
			Handler:    _Querier_FindTraceByID_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "tempo.proto",
}

func (m *TraceByIDRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TraceByIDRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceByIDRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.QueryIngesters {
		i--
		if m.QueryIngesters {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x20
	}
	if len(m.BlockEnd) > 0 {
		i -= len(m.BlockEnd)
		copy(dAtA[i:], m.BlockEnd)
		i = encodeVarintTempo(dAtA, i, uint64(len(m.BlockEnd)))
		i--
		dAtA[i] = 0x1a
	}
	if len(m.BlockStart) > 0 {
		i -= len(m.BlockStart)
		copy(dAtA[i:], m.BlockStart)
		i = encodeVarintTempo(dAtA, i, uint64(len(m.BlockStart)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.TraceID) > 0 {
		i -= len(m.TraceID)
		copy(dAtA[i:], m.TraceID)
		i = encodeVarintTempo(dAtA, i, uint64(len(m.TraceID)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *TraceByIDResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TraceByIDResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceByIDResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Trace != nil {
		{
			size, err := m.Trace.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintTempo(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Trace) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Trace) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Trace) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Batches) > 0 {
		for iNdEx := len(m.Batches) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Batches[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTempo(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *PushRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PushRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PushRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Batch != nil {
		{
			size, err := m.Batch.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintTempo(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *PushResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PushResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PushResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func (m *PushBytesRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PushBytesRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PushBytesRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Requests) > 0 {
		for iNdEx := len(m.Requests) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.Requests[iNdEx])
			copy(dAtA[i:], m.Requests[iNdEx])
			i = encodeVarintTempo(dAtA, i, uint64(len(m.Requests[iNdEx])))
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func encodeVarintTempo(dAtA []byte, offset int, v uint64) int {
	offset -= sovTempo(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *TraceByIDRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.TraceID)
	if l > 0 {
		n += 1 + l + sovTempo(uint64(l))
	}
	l = len(m.BlockStart)
	if l > 0 {
		n += 1 + l + sovTempo(uint64(l))
	}
	l = len(m.BlockEnd)
	if l > 0 {
		n += 1 + l + sovTempo(uint64(l))
	}
	if m.QueryIngesters {
		n += 2
	}
	return n
}

func (m *TraceByIDResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Trace != nil {
		l = m.Trace.Size()
		n += 1 + l + sovTempo(uint64(l))
	}
	return n
}

func (m *Trace) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Batches) > 0 {
		for _, e := range m.Batches {
			l = e.Size()
			n += 1 + l + sovTempo(uint64(l))
		}
	}
	return n
}

func (m *PushRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Batch != nil {
		l = m.Batch.Size()
		n += 1 + l + sovTempo(uint64(l))
	}
	return n
}

func (m *PushResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func (m *PushBytesRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Requests) > 0 {
		for _, b := range m.Requests {
			l = len(b)
			n += 1 + l + sovTempo(uint64(l))
		}
	}
	return n
}

func sovTempo(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTempo(x uint64) (n int) {
	return sovTempo(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *TraceByIDRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTempo
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TraceByIDRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TraceByIDRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TraceID", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthTempo
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTempo
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.TraceID = append(m.TraceID[:0], dAtA[iNdEx:postIndex]...)
			if m.TraceID == nil {
				m.TraceID = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BlockStart", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTempo
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTempo
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.BlockStart = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BlockEnd", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTempo
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTempo
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.BlockEnd = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field QueryIngesters", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.QueryIngesters = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipTempo(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTempo
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *TraceByIDResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTempo
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TraceByIDResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TraceByIDResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Trace", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTempo
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTempo
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Trace == nil {
				m.Trace = &Trace{}
			}
			if err := m.Trace.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTempo(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTempo
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Trace) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTempo
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Trace: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Trace: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Batches", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTempo
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTempo
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Batches = append(m.Batches, &v1.ResourceSpans{})
			if err := m.Batches[len(m.Batches)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTempo(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTempo
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PushRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTempo
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PushRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PushRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Batch", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTempo
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTempo
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Batch == nil {
				m.Batch = &v1.ResourceSpans{}
			}
			if err := m.Batch.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTempo(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTempo
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PushResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTempo
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PushResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PushResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipTempo(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTempo
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PushBytesRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTempo
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PushBytesRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PushBytesRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Requests", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthTempo
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTempo
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Requests = append(m.Requests, make([]byte, postIndex-iNdEx))
			copy(m.Requests[len(m.Requests)-1], dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTempo(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTempo
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTempo(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTempo
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTempo
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTempo
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTempo
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTempo
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTempo        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTempo          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTempo = fmt.Errorf("proto: unexpected end of group")
)
