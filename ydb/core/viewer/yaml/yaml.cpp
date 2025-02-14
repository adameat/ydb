#include <unordered_set>
#include <util/stream/str.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/wrappers.pb.h>
#include <google/protobuf/util/time_util.h>
#include "yaml.h"

#ifdef GetMessage
#undef GetMessage
#endif

using namespace ::google::protobuf;

YAML::Node TProtoToYaml::ProtoToYamlSchema(const EnumDescriptor* descriptor, const TEnumSettings& enumSettings) {
    YAML::Node to;
    auto enm = to["enum"];
    auto valueCount = descriptor->value_count();
    TString defaultValue;
    for (int i = 0; i < valueCount; ++i) {
        auto enumValueDescriptor = descriptor->value(i);
        auto enumName = enumValueDescriptor->name();
        if (enumSettings.ConvertToLowerCase) {
            enumName = to_lower(enumName);
        }
        if (!defaultValue) {
            defaultValue = enumName;
            if (enumSettings.SkipDefaultValue) {
                continue;
            }
        }
        enm.push_back(enumName);
    }
    if (defaultValue && !enumSettings.SkipDefaultValue) {
        to["default"] = defaultValue;
    }
    return to;
}

YAML::Node TProtoToYaml::ProtoToYamlSchema(const FieldDescriptor* descriptor, std::unordered_set<const Descriptor*>& descriptors) {
    if (descriptor->is_repeated() && !descriptor->is_map()) {
        YAML::Node to;
        to["type"] = "array";
        to["items"] = ProtoToYamlSchemaNoRepeated(descriptor, descriptors);
        return to;
    } else {
        return ProtoToYamlSchemaNoRepeated(descriptor, descriptors);
    }
}

YAML::Node TProtoToYaml::ProtoToYamlSchemaNoRepeated(const FieldDescriptor* descriptor, std::unordered_set<const Descriptor*>& descriptors) {
    YAML::Node to;
    if (descriptor->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
        const Descriptor* message = descriptor->message_type();
        if (message->options().map_entry()) {
            to["type"] = "object";
            to["additionalProperties"]["type"] = GetFieldTypeName(message->map_value());
        } else {
            if (message->full_name() == Duration::descriptor()->full_name()) {
                to["type"] = "string";
                to["example"] = "3600s";
            } else if(message->full_name() == Timestamp::descriptor()->full_name()) {
                to["type"] = "string";
                to["format"] = "date-time";
                to["example"] = "2025-04-09T00:00:00Z";
            } else if (message->full_name() == BoolValue::descriptor()->full_name()) {
                to["type"] = "boolean";
            } else if (message->full_name() == StringValue::descriptor()->full_name()) {
                to["type"] = "string";
            } else if (message->full_name() == Int64Value::descriptor()->full_name()) {
                to["type"] = "integer";
                to["format"] = "int64";
            } else if (descriptors.insert(message).second) {
                to = ProtoToYamlSchema(message, descriptors);
                descriptors.erase(message);
            }
        }
    } else {
        to["type"] = GetFieldTypeName(descriptor);
        switch (descriptor->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            to["format"] = "int32";
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            to["format"] = "uint32";
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            to["format"] = "int64";
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            to["format"] = "uint64";
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            to["format"] = "float";
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            to["format"] = "double";
            break;
        default:
            break;
        }

        if (descriptor->cpp_type() == FieldDescriptor::CPPTYPE_ENUM) {
            to = ProtoToYamlSchema(descriptor->enum_type());
        }
    }
    return to;
}

YAML::Node TProtoToYaml::ProtoToYamlSchema(const Descriptor* descriptor, std::unordered_set<const Descriptor*>& descriptors) {
    if (descriptor == nullptr) {
        return {};
    }
    YAML::Node to;
    to["type"] = "object";
    to["title"] = descriptor->name();
    int fields = descriptor->field_count();
    if (fields > 0) {
        auto properties = to["properties"];
        int oneofFields = descriptor->oneof_decl_count();
        for (int idx = 0; idx < oneofFields; ++idx) {
            const OneofDescriptor* oneofDescriptor = descriptor->oneof_decl(idx);
            if (oneofDescriptor->name().StartsWith("_") || oneofDescriptor->is_synthetic()) {
                continue;
            }
            auto oneOf = properties[oneofDescriptor->name()]["oneOf"];
            for (int oneOfIdx = 0; oneOfIdx < oneofDescriptor->field_count(); ++oneOfIdx) {
                const FieldDescriptor* fieldDescriptor = oneofDescriptor->field(oneOfIdx);
                oneOf[fieldDescriptor->name()] = ProtoToYamlSchema(fieldDescriptor, descriptors);
            }
        }
        for (int idx = 0; idx < fields; ++idx) {
            const FieldDescriptor* fieldDescriptor = descriptor->field(idx);
            if (fieldDescriptor->real_containing_oneof() != nullptr) {
                continue;
            }
            properties[fieldDescriptor->name()] = ProtoToYamlSchema(fieldDescriptor, descriptors);
        }
    }
    return to;
}

TString TProtoToYaml::GetFieldTypeName(const ::google::protobuf::FieldDescriptor* descriptor) {
    switch (descriptor->cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
    case FieldDescriptor::CPPTYPE_UINT32:
        return "integer";
    case FieldDescriptor::CPPTYPE_INT64:
    case FieldDescriptor::CPPTYPE_UINT64:
        return "string"; // because of JS compatibility (JavaScript could not handle large numbers (bigger than 2^53))
    case FieldDescriptor::CPPTYPE_STRING:
        return "string";
    case FieldDescriptor::CPPTYPE_FLOAT:
    case FieldDescriptor::CPPTYPE_DOUBLE:
        return "number";
    case FieldDescriptor::CPPTYPE_BOOL:
        return "boolean";
    case FieldDescriptor::CPPTYPE_ENUM:
        return "string";
    case FieldDescriptor::CPPTYPE_MESSAGE:
        return "object";
    default:
        return "unknown";
    }
}

YAML::Node TProtoToYaml::ProtoToYamlSchema(const Descriptor* descriptor) {
    std::unordered_set<const Descriptor*> descriptors;
    return ProtoToYamlSchema(descriptor, descriptors);
}
