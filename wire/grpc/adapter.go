package grpc

import (
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func fromAnypb(p *anypb.Any) any {
	if p == nil {
		return nil
	}
	switch p.TypeUrl {
	case "type.googleapis.com/google.protobuf.DoubleValue":
		var doubleVal wrapperspb.DoubleValue
		if err := p.UnmarshalTo(&doubleVal); err != nil {
			return nil
		}
		return doubleVal.Value
	case "type.googleapis.com/google.protobuf.StringValue":
		var strVal wrapperspb.StringValue
		if err := p.UnmarshalTo(&strVal); err != nil {
			return nil
		}
		return strVal.Value
	case "type.googleapis.com/google.protobuf.Int32Value":
		var int32Val wrapperspb.Int32Value
		if err := p.UnmarshalTo(&int32Val); err != nil {
			return nil
		}
		return int32Val.Value
	case "type.googleapis.com/google.protobuf.UInt32Value":
		var uint32Val wrapperspb.UInt32Value
		if err := p.UnmarshalTo(&uint32Val); err != nil {
			return nil
		}
		return uint32Val.Value
	case "type.googleapis.com/google.protobuf.Int64Value":
		var int64Val wrapperspb.Int64Value
		if err := p.UnmarshalTo(&int64Val); err != nil {
			return nil
		}
		return int64Val.Value
	case "type.googleapis.com/google.protobuf.UInt64Value":
		var uint64Val wrapperspb.UInt64Value
		if err := p.UnmarshalTo(&uint64Val); err != nil {
			return nil
		}
		return uint64Val.Value
	case "type.googleapis.com/google.protobuf.FloatValue":
		var floatVal wrapperspb.FloatValue
		if err := p.UnmarshalTo(&floatVal); err != nil {
			return nil
		}
		return floatVal.Value
	case "type.googleapis.com/google.protobuf.BoolValue":
		var boolVal wrapperspb.BoolValue
		if err := p.UnmarshalTo(&boolVal); err != nil {
			return nil
		}
		return boolVal.Value
	case "type.googleapis.com/google.protobuf.BytesValue":
		var bytesVal wrapperspb.BytesValue
		if err := p.UnmarshalTo(&bytesVal); err != nil {
			return nil
		}
		return bytesVal.Value
	case "type.googleapis.com/google.protobuf.Timestamp":
		var tsVal timestamppb.Timestamp
		if err := p.UnmarshalTo(&tsVal); err != nil {
			return nil
		}
		return tsVal.AsTime()
	case "type.googleapis.com/google.protobuf.Empty":
		return nil
	default:
		slog.Warn("unsupported Any type", "type_url", p.TypeUrl)
		var strVal wrapperspb.StringValue
		if err := p.UnmarshalTo(&strVal); err != nil {
			return nil
		}
		return strVal.Value
	}
}

func toAnypb(val any) (*anypb.Any, error) {
	var m proto.Message
	switch v := val.(type) {
	case *any:
		m = wrapperspb.String(fmt.Sprint(*v))
	case string:
		m = wrapperspb.String(v)
	case float32:
		m = wrapperspb.Float(v)
	case float64:
		m = wrapperspb.Double(v)
	case int32:
		m = wrapperspb.Int32(v)
	case int64:
		m = wrapperspb.Int64(v)
	case int:
		m = wrapperspb.Int64(int64(v))
	case uint32:
		m = wrapperspb.UInt32(v)
	case uint64:
		m = wrapperspb.UInt64(v)
	case uint:
		m = wrapperspb.UInt64(uint64(v))
	case bool:
		m = wrapperspb.Bool(v)
	case []byte:
		m = wrapperspb.Bytes(v)
	case time.Time:
		m = timestamppb.New(v)
	case nil:
		m = &emptypb.Empty{}
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
	return anypb.New(m)
}
