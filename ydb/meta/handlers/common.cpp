#include "common.h"
#include <ydb/meta/meta.h>
#include <ydb/meta/tokens/meta_tokens.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb-cpp-sdk/client/resources/ydb_resources.h>

namespace NMeta {

bool THandlerActorYdb::IsRetryableError(const Ydb::Operations::Operation& operation) {
    for (const Ydb::Issue::IssueMessage& issue : operation.issues()) {
        if (issue.message().find("database unknown") != TString::npos) {
            return true;
        }
        if (issue.message().find("#200802") != TString::npos) {
            return true;
        }
    }
    return false;
}

NHttp::THttpOutgoingResponsePtr THandlerActorYdb::CreateStatusResponse(NHttp::THttpIncomingRequestPtr request, const NYdb::TStatus& status) {
    Ydb::Operations::Operation operation;
    operation.set_status(static_cast<Ydb::StatusIds_StatusCode>(status.GetStatus()));
    NYdb::NIssue::IssuesToMessage(status.GetIssues(), operation.mutable_issues());
    return CreateStatusResponse(request, operation);
}

NHttp::THttpOutgoingResponsePtr THandlerActorYdb::CreateStatusResponse(NHttp::THttpIncomingRequestPtr request, const Ydb::Operations::Operation& operation) {
    TStringBuf status = "503";
    TStringBuf message = "Service Unavailable";
    switch ((int)operation.status()) {
    case Ydb::StatusIds::SUCCESS:
        status = "200";
        message = "OK";
        break;
    case Ydb::StatusIds::UNAUTHORIZED:
    case (int)NYdb::EStatus::CLIENT_UNAUTHENTICATED:
        status = "401";
        message = "Unauthorized";
        break;
    case Ydb::StatusIds::BAD_REQUEST:
    case Ydb::StatusIds::SCHEME_ERROR:
    case Ydb::StatusIds::GENERIC_ERROR:
    case Ydb::StatusIds::BAD_SESSION:
    case Ydb::StatusIds::PRECONDITION_FAILED:
    case Ydb::StatusIds::ALREADY_EXISTS:
    case Ydb::StatusIds::SESSION_EXPIRED:
    case Ydb::StatusIds::UNDETERMINED:
    case Ydb::StatusIds::ABORTED:
    case Ydb::StatusIds::UNSUPPORTED:
        status = "400";
        message = "Bad Request";
        break;
    case Ydb::StatusIds::NOT_FOUND:
        status = "404";
        message = "Not Found";
        break;
    case Ydb::StatusIds::OVERLOADED:
        status = "429";
        message = "Overloaded";
        break;
    case Ydb::StatusIds::INTERNAL_ERROR:
        status = "500";
        message = "Internal Server Error";
        break;
    case Ydb::StatusIds::UNAVAILABLE:
        status = "503";
        message = "Service Unavailable";
        break;
    case Ydb::StatusIds::TIMEOUT:
    case (int)NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED:
        status = "504";
        message = "Gateway Time-out";
        break;
    default:
        break;
    }
    TStringStream stream;
    NProtobufJson::Proto2Json(operation, stream);
    return request->CreateResponse(status, message, "application/json", stream.Str());
}

NHttp::THttpOutgoingResponsePtr THandlerActorYdb::CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TEvPrivate::TEvErrorResponse* error) {
    NJson::TJsonValue json;
    json["message"] = error->Message;
    TString body = NJson::WriteJson(json, false);
    return request->CreateResponse(error->Status, error->Message, "application/json", body);
}

NHttp::THttpOutgoingResponsePtr THandlerActorYdb::CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TString& error) {
    NJson::TJsonValue json;
    json["message"] = error;
    TString body = NJson::WriteJson(json, false);
    return request->CreateResponseServiceUnavailable(body, "application/json");
}

TString THandlerActorYdb::ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetPrimitiveType()) {
        case NYdb::EPrimitiveType::Bool:
            return TStringBuilder() << valueParser.GetBool();
        case NYdb::EPrimitiveType::Int8:
            return TStringBuilder() << valueParser.GetInt8();
        case NYdb::EPrimitiveType::Uint8:
            return TStringBuilder() << valueParser.GetUint8();
        case NYdb::EPrimitiveType::Int16:
            return TStringBuilder() << valueParser.GetInt16();
        case NYdb::EPrimitiveType::Uint16:
            return TStringBuilder() << valueParser.GetUint16();
        case NYdb::EPrimitiveType::Int32:
            return TStringBuilder() << valueParser.GetInt32();
        case NYdb::EPrimitiveType::Uint32:
            return TStringBuilder() << valueParser.GetUint32();
        case NYdb::EPrimitiveType::Int64:
            return TStringBuilder() << valueParser.GetInt64();
        case NYdb::EPrimitiveType::Uint64:
            return TStringBuilder() << valueParser.GetUint64();
        case NYdb::EPrimitiveType::Float:
            return TStringBuilder() << valueParser.GetFloat();
        case NYdb::EPrimitiveType::Double:
            return TStringBuilder() << valueParser.GetDouble();
        case NYdb::EPrimitiveType::Utf8:
            return TStringBuilder() << valueParser.GetUtf8();
        case NYdb::EPrimitiveType::Date:
            return TStringBuilder() << valueParser.GetDate().ToString();
        case NYdb::EPrimitiveType::Datetime:
            return TStringBuilder() << valueParser.GetDatetime().ToString();
        case NYdb::EPrimitiveType::Timestamp:
            return TStringBuilder() << valueParser.GetTimestamp().ToString();
        case NYdb::EPrimitiveType::Interval:
            return TStringBuilder() << valueParser.GetInterval();
        case NYdb::EPrimitiveType::Date32:
            return TStringBuilder() << valueParser.GetDate32();
        case NYdb::EPrimitiveType::Datetime64:
            return TStringBuilder() << valueParser.GetDatetime64();
        case NYdb::EPrimitiveType::Timestamp64:
            return TStringBuilder() << valueParser.GetTimestamp64();
        case NYdb::EPrimitiveType::Interval64:
            return TStringBuilder() << valueParser.GetInterval64();
        case NYdb::EPrimitiveType::TzDate:
            return TStringBuilder() << valueParser.GetTzDate();
        case NYdb::EPrimitiveType::TzDatetime:
            return TStringBuilder() << valueParser.GetTzDatetime();
        case NYdb::EPrimitiveType::TzTimestamp:
            return TStringBuilder() << valueParser.GetTzTimestamp();
        case NYdb::EPrimitiveType::String:
            return TStringBuilder() << Base64Encode(valueParser.GetString());
        case NYdb::EPrimitiveType::Yson:
            return TStringBuilder() << valueParser.GetYson();
        case NYdb::EPrimitiveType::Json:
            return TStringBuilder() << valueParser.GetJson();
        case NYdb::EPrimitiveType::JsonDocument:
            return TStringBuilder() << valueParser.GetJsonDocument();
        case NYdb::EPrimitiveType::DyNumber:
            return TStringBuilder() << valueParser.GetDyNumber();
        case NYdb::EPrimitiveType::Uuid:
            return TStringBuilder() << "<uuid not implemented>";
    }
}

TString THandlerActorYdb::ColumnValueToString(const NYdb::TValue& value) {
    NYdb::TValueParser valueParser(value);
    return ColumnValueToString(valueParser);
}

TString THandlerActorYdb::ColumnValueToString(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Primitive:
            return ColumnPrimitiveValueToString(valueParser);

        case NYdb::TTypeParser::ETypeKind::Optional:
            valueParser.OpenOptional();
            if (valueParser.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive) {
                if (valueParser.IsNull()) {
                    return "";
                } else {
                    return ColumnPrimitiveValueToString(valueParser);
                }
            }

            return TStringBuilder() << NYdb::TTypeParser::ETypeKind::Optional;

        default:
            return TStringBuilder() << valueParser.GetKind();
    }
}

NJson::TJsonValue THandlerActorYdb::ColumnPrimitiveValueToJsonValue(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetPrimitiveType()) {
        case NYdb::EPrimitiveType::Bool:
            return valueParser.GetBool();
        case NYdb::EPrimitiveType::Int8:
            return valueParser.GetInt8();
        case NYdb::EPrimitiveType::Uint8:
            return valueParser.GetUint8();
        case NYdb::EPrimitiveType::Int16:
            return valueParser.GetInt16();
        case NYdb::EPrimitiveType::Uint16:
            return valueParser.GetUint16();
        case NYdb::EPrimitiveType::Int32:
            return valueParser.GetInt32();
        case NYdb::EPrimitiveType::Uint32:
            return valueParser.GetUint32();
        case NYdb::EPrimitiveType::Int64:
            return TStringBuilder() << valueParser.GetInt64();
        case NYdb::EPrimitiveType::Uint64:
            return TStringBuilder() << valueParser.GetUint64();
        case NYdb::EPrimitiveType::Float:
            return valueParser.GetFloat();
        case NYdb::EPrimitiveType::Double:
            return valueParser.GetDouble();
        case NYdb::EPrimitiveType::Utf8:
            return valueParser.GetUtf8();
        case NYdb::EPrimitiveType::Date:
            return valueParser.GetDate().ToString();
        case NYdb::EPrimitiveType::Datetime:
            return valueParser.GetDatetime().ToString();
        case NYdb::EPrimitiveType::Timestamp:
            return valueParser.GetTimestamp().ToString();
        case NYdb::EPrimitiveType::Interval:
            return TStringBuilder() << valueParser.GetInterval();
        case NYdb::EPrimitiveType::Date32:
            return valueParser.GetDate32();
        case NYdb::EPrimitiveType::Datetime64:
            return valueParser.GetDatetime64();
        case NYdb::EPrimitiveType::Timestamp64:
            return valueParser.GetTimestamp64();
        case NYdb::EPrimitiveType::Interval64:
            return valueParser.GetInterval64();
        case NYdb::EPrimitiveType::TzDate:
            return valueParser.GetTzDate();
        case NYdb::EPrimitiveType::TzDatetime:
            return valueParser.GetTzDatetime();
        case NYdb::EPrimitiveType::TzTimestamp:
            return valueParser.GetTzTimestamp();
        case NYdb::EPrimitiveType::String:
            return Base64Encode(valueParser.GetString());
        case NYdb::EPrimitiveType::Yson:
            return valueParser.GetYson();
        case NYdb::EPrimitiveType::Json:
            return valueParser.GetJson();
        case NYdb::EPrimitiveType::JsonDocument:
            return valueParser.GetJsonDocument();
        case NYdb::EPrimitiveType::DyNumber:
            return valueParser.GetDyNumber();
        case NYdb::EPrimitiveType::Uuid:
            return "<uuid not implemented>";
    }
}

NJson::TJsonValue THandlerActorYdb::ColumnValueToJsonValue(NYdb::TValueParser& valueParser) {
    switch (valueParser.GetKind()) {
    case NYdb::TTypeParser::ETypeKind::Primitive:
        return ColumnPrimitiveValueToJsonValue(valueParser);
    case NYdb::TTypeParser::ETypeKind::Optional: {
        NJson::TJsonValue jsonValue;
        valueParser.OpenOptional();
        if (valueParser.IsNull()) {
            jsonValue = NJson::JSON_NULL;
        } else {
            jsonValue = ColumnValueToJsonValue(valueParser);
        }
        valueParser.CloseOptional();
        return jsonValue;
    }
    case NYdb::TTypeParser::ETypeKind::Tuple: {
        NJson::TJsonValue jsonArray;
        jsonArray.SetType(NJson::JSON_ARRAY);
        valueParser.OpenTuple();
        while (valueParser.TryNextElement()) {
            jsonArray.AppendValue(ColumnValueToJsonValue(valueParser));
        }
        valueParser.CloseTuple();
        return jsonArray;
    }
    default:
        return NJson::JSON_UNDEFINED;
    }
}

NJson::TJsonValue THandlerActorYdb::RowToJsonValue(const NYdb::TResultSet& rs, NYdb::TResultSetParser& rsParser) {
    NJson::TJsonValue row;
    const auto& meta = rs.GetColumnsMeta();
    for (size_t columnNum = 0; columnNum < meta.size(); ++columnNum) {
        row[meta[columnNum].Name] = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
    }
    return row;
}

TString THandlerActorYdb::GetApiUrl(TString balancer, const TString& uri) {
    if (!balancer.StartsWith("http://") && !balancer.StartsWith("https://")) {
        if (balancer.find('/') == TString::npos) {
            balancer += ":8765/viewer";
        }
        if (balancer.StartsWith("/")) {
            if (auto metaYdb = InstanceYdbMeta.lock()) {
                balancer = metaYdb->LocalEndpoint + balancer;
            }
        } else {
            balancer = "http://" + balancer;
        }
    }
    return balancer + uri;
}

bool THandlerActorYdb::isalnum(const TString& str) {
    for (char c : str) {
        if (!std::isalnum(c)) {
            return false;
        }
    }
    return true;
}

bool THandlerActorYdb::IsValidDatabaseId(const TString& databaseId) {
    return !databaseId.empty() && databaseId.size() <= 20 && isalnum(databaseId);
}

bool THandlerActorYdb::IsValidParameterName(const TString& param) {
    for (char c : param) {
        if (c != '$' && c != '_' && !std::isalnum(c)) {
            return false;
        }
    }
    return true;
}

THandlerActorYdb::TParameters::TParameters(const NHttp::THttpIncomingRequestPtr& request)
    : Success(true)
    , UrlParameters(request->URL)
{
    TStringBuf contentTypeHeader = request->ContentType;
    TStringBuf contentType = contentTypeHeader.NextTok(';');
    if (contentType == "application/json") {
        Success = NJson::ReadJsonTree(request->Body, &THandlerActorYdb::JsonReaderConfig, &PostData);
    }
}

TString THandlerActorYdb::TParameters::GetContentParameter(TStringBuf name) const {
    if (PostData.IsDefined()) {
        const NJson::TJsonValue* value;
        if (PostData.GetValuePointer(name, &value)) {
            return value->GetStringRobust();
        }
    }
    return TString();
}

TString THandlerActorYdb::TParameters::GetUrlParameter(TStringBuf name) const {
    return UrlParameters[name];
}

TString THandlerActorYdb::TParameters::operator [](TStringBuf name) const {
    TString value = GetUrlParameter(name);
    if (value.empty()) {
        value = GetContentParameter(name);
    }
    return value;
}

void THandlerActorYdb::TParameters::ParamsToProto(google::protobuf::Message& proto) const {
    using google::protobuf::Descriptor;
    using google::protobuf::Reflection;
    using google::protobuf::FieldDescriptor;
    using google::protobuf::EnumDescriptor;
    using google::protobuf::EnumValueDescriptor;
    const Descriptor& descriptor = *proto.GetDescriptor();
    const Reflection& reflection = *proto.GetReflection();
    for (int idx = 0; idx < descriptor.field_count(); ++idx) {
        const FieldDescriptor* field = descriptor.field(idx);
        TString name = field->name();
        TString value = UrlParameters[name];
        if (!value.empty()) {
            FieldDescriptor::CppType type = field->cpp_type();
            switch (type) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection.SetInt32(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection.SetInt64(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection.SetUInt32(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection.SetUInt64(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection.SetDouble(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection.SetFloat(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection.SetBool(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_ENUM: {
                const EnumDescriptor* enumDescriptor = field->enum_type();
                const EnumValueDescriptor* enumValueDescriptor = enumDescriptor->FindValueByName(value);
                int number = 0;
                if (enumValueDescriptor == nullptr && TryFromString(value, number)) {
                    enumValueDescriptor = enumDescriptor->FindValueByNumber(number);
                }
                if (enumValueDescriptor != nullptr) {
                    reflection.SetEnum(&proto, field, enumValueDescriptor);
                }
                break;
            }
            case FieldDescriptor::CPPTYPE_STRING:
                reflection.SetString(&proto, field, value);
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                break;
            }
        }
    }
}

TString THandlerActorYdb::TRequest::GetAuthToken() const {
    NHttp::THeaders headers(Request->Headers);
    return GetAuthToken(headers);
}

TString THandlerActorYdb::TRequest::GetAuthToken(const NHttp::THeaders& headers) const {
    NHttp::TCookies cookies(headers["Cookie"]);
    TStringBuf authorization = headers["Authorization"];
    if (!authorization.empty()) {
        TStringBuf scheme = authorization.NextTok(' ');
        if (scheme == "OAuth" || scheme == "Bearer") {
            return TString(authorization);
        }
    }
    TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
    if (!subjectToken.empty()) {
        return TString(subjectToken);
    }
    /* TStringBuf sessionId = cookies["Session_id"];
    if (!sessionId.empty()) {
        return BlackBoxTokenFromSessionId(sessionId);
    } */
    return TString();
}

TString THandlerActorYdb::TRequest::GetAuthTokenForIAM() const {
    NHttp::THeaders headers(Request->Headers);
    return GetAuthTokenForIAM(headers);
}

TString THandlerActorYdb::TRequest::GetAuthTokenForIAM(const NHttp::THeaders& headers) const {
    TStringBuf authorization = headers["Authorization"];
    if (!authorization.empty()) {
        TStringBuf scheme = authorization.NextTok(' ');
        if (scheme == "Bearer") {
            return "Bearer " + TString(authorization);
        }
    }
    TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
    if (!subjectToken.empty()) {
        return "Bearer " + TString(subjectToken);
    }
    return TString();
}

void THandlerActorYdb::TRequest::SetHeader(NYdbGrpc::TCallMeta& meta, const TString& name, const TString& value) {
    for (auto& [exname, exvalue] : meta.Aux) {
        if (exname == name) {
            exvalue = value;
            return;
        }
    }
    meta.Aux.emplace_back(name, value);
}

void THandlerActorYdb::TRequest::ForwardHeaders(NYdbGrpc::TCallMeta& meta) const {
    NHttp::THeaders headers(Request->Headers);
    TString token = GetAuthToken(headers);
    if (!token.empty()) {
        SetHeader(meta, "authorization", "Bearer " + token);
        SetHeader(meta, NYdb::YDB_AUTH_TICKET_HEADER, token);
    }
    ForwardHeader(headers, meta, "x-request-id");
}

void THandlerActorYdb::TRequest::ForwardHeaders(NHttp::THttpOutgoingRequestPtr& request) const {
    NHttp::THeaders headers(Request->Headers);
    TString token = GetAuthToken(headers);
    if (!token.empty()) {
        request->Set("authorization", "Bearer " + token);
        request->Set(NYdb::YDB_AUTH_TICKET_HEADER, token);
    }
    ForwardHeader(headers, request, "x-request-id");
}

void THandlerActorYdb::TRequest::ForwardHeadersOnlyForIAM(NYdbGrpc::TCallMeta& meta) const {
    NHttp::THeaders headers(Request->Headers);
    TString token;
    TStringBuf srcAuthorization = headers["Authorization"];
    if (!srcAuthorization.empty()) {
        TStringBuf scheme = srcAuthorization.NextTok(' ');
        if (scheme == "Bearer") {
            token = srcAuthorization;
        }
    }
    TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
    if (!subjectToken.empty()) {
        token = subjectToken;
    }
    if (!token.empty()) {
        SetHeader(meta, "authorization", "Bearer " + token);
        SetHeader(meta, NYdb::YDB_AUTH_TICKET_HEADER, "Bearer " + token);
    }
    ForwardHeader(headers, meta, "x-request-id");
}

void THandlerActorYdb::TRequest::ForwardHeader(const NHttp::THeaders& header, NYdbGrpc::TCallMeta& meta, TStringBuf name) const {
    TStringBuf value(header[name]);
    if (!value.empty()) {
        SetHeader(meta, TString(name), TString(value));
    }
}

void THandlerActorYdb::TRequest::ForwardHeader(const NHttp::THeaders& header, NHttp::THttpOutgoingRequestPtr& request, TStringBuf name) const {
    TStringBuf value(header[name]);
    if (!value.empty()) {
        request->Set(name, value);
    }
}

void THandlerActorYdb::TRequest::ForwardHeadersOnlyForIAM(NHttp::THttpOutgoingRequestPtr& request) const {
    NHttp::THeaders headers(Request->Headers);
    TString token;
    TStringBuf srcAuthorization = headers["Authorization"];
    if (!srcAuthorization.empty()) {
        TStringBuf scheme = srcAuthorization.NextTok(' ');
        if (scheme == "Bearer") {
            token = srcAuthorization;
        }
    }
    ForwardHeader(headers, request, "x-yacloud-subjecttoken");
    if (!token.empty()) {
        request->Set("authorization", "Bearer " + token);
        request->Set(NYdb::YDB_AUTH_TICKET_HEADER, "Bearer " + token);
    }
    ForwardHeader(headers, request, "x-request-id");
}

TString THandlerActorYdb::GetAuthHeaderValue(const TString& tokenName) {
    NMeta::TMetaTokenator* tokenator = MetaAppData()->Tokenator;
    TString authHeaderValue;
    if (tokenator && !tokenName.empty()) {
        authHeaderValue = tokenator->GetToken(tokenName);
    }
    return authHeaderValue;
}

TString SnakeToCamelCase(TString name) {
    name[0] = static_cast<char>(tolower(name[0]));
    size_t max = name.size() - 1;
    for (size_t i = 1; i < max;) {
        if (name[i] == '_') {
            name[i] = static_cast<char>(toupper(name[i + 1]));
            name.erase(i + 1, 1);
            max--;
        } else {
            ++i;
        }
    }
    return name;
}

TString SnakeToCCamelCase(TString name) {
    name[0] = static_cast<char>(toupper(name[0]));
    size_t max = name.size() - 1;
    for (size_t i = 1; i < max;) {
        if (name[i] == '_') {
            name[i] = static_cast<char>(toupper(name[i + 1]));
            name.erase(i + 1, 1);
            max--;
        } else {
            ++i;
        }
    }
    return name;
}

TString CamelToSnakeCase(TString name) {
    size_t max = name.size() - 1;
    name[0] = static_cast<char>(tolower(name[0]));
    for (size_t i = 1; i < max;) {
        if (isupper(name[i])) {
            name.insert(i, "_");
            ++i;
            name[i] = static_cast<char>(tolower(name[i]));
            ++max;
        } else {
            ++i;
        }
    }
    return name;
}

TString SnakeToCamelCaseProtoConverter(const google::protobuf::FieldDescriptor& field) {
    return SnakeToCamelCase(field.name());
}

TString SnakeToCCamelCaseProtoConverter(const google::protobuf::FieldDescriptor& field) {
    return SnakeToCCamelCase(field.name());
}

NJson::TJsonReaderConfig THandlerActorYdb::JsonReaderConfig;
NJson::TJsonWriterConfig THandlerActorYdb::JsonWriterConfig;
NProtobufJson::TJson2ProtoConfig THandlerActorYdb::Json2ProtoConfig = NProtobufJson::TJson2ProtoConfig()
//        .SetNameGenerator(SnakeToCamelCaseProtoConverter)
        .SetMapAsObject(true)
        .SetAllowString2TimeConversion(true);
NProtobufJson::TProto2JsonConfig THandlerActorYdb::Proto2JsonConfig = NProtobufJson::TProto2JsonConfig()
//        .SetNameGenerator(SnakeToCamelCaseProtoConverter)
        .SetMapAsObject(true)
        .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumValueMode::EnumName)
        .SetConvertAny(true);

}
