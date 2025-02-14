#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/string_utils/base64/base64.h>
#include "ydb.h"
#include "appdata.h"
#include <ydb/meta/tokens/meta_tokens.h>

namespace NMeta {

TYdbLocation::TYdbLocation(const TString& endpoint)
    : Endpoint(endpoint)
{
}

NYdb::TDriverConfig TYdbLocation::GetDriverConfig() {
    TStringBuf scheme = "grpc";
    TStringBuf host;
    TStringBuf uri;
    TStringBuf tokenName;
    CrackEndpoint(Endpoint, scheme, tokenName, host, uri);
    NYdb::TDriverConfig config(TString(scheme) + "://" + host);
    config.SetDatabase(TString(uri));
    if (tokenName) {
        if (MetaAppData()->Tokenator) {
            TString token = MetaAppData()->Tokenator->GetToken(TString(tokenName));
            if (token) {
                config.SetAuthToken(token);
            }
        }
    }
    return config;
}

NYdb::TDriver& TYdbLocation::GetDriver() {
    return Driver.GetRef([this](){ return new NYdb::TDriver(GetDriverConfig()); });
}

NYdb::NQuery::TAsyncExecuteQueryResult TYdbLocation::ExecuteQuery(const TString& query, const NYdb::TParams& params, const NYdb::NQuery::TExecuteQuerySettings& settings) {
    return std::make_unique<NYdb::NQuery::TQueryClient>(GetDriver())->ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params, settings);
}

TString TYdbLocation::ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser) {
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

TString TYdbLocation::ColumnValueToString(const NYdb::TValue& value) {
    NYdb::TValueParser valueParser(value);
    return ColumnValueToString(valueParser);
}

TString TYdbLocation::ColumnValueToString(NYdb::TValueParser& valueParser) {
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

NJson::TJsonValue TYdbLocation::ColumnPrimitiveValueToJsonValue(NYdb::TValueParser& valueParser) {
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

NJson::TJsonValue TYdbLocation::ColumnValueToJsonValue(NYdb::TValueParser& valueParser) {
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

bool CrackEndpoint(TStringBuf endpoint, TStringBuf& scheme, TStringBuf& tokenName, TStringBuf& host, TStringBuf& uri) {
    NHttp::CrackURL(endpoint, scheme, host, uri);
    tokenName = host.RSplitOff('@');
    return true;
}

TString ConstructEndpoint(const TString& endpoint, const TString& externalTokenName) {
    TStringBuf scheme = "grpc";
    TStringBuf host;
    TStringBuf url;
    TStringBuf tokenName;
    CrackEndpoint(endpoint, scheme, tokenName, host, url);
    if (!tokenName && externalTokenName) {
        tokenName = externalTokenName;
    }
    return TString(scheme) + "://" + (tokenName ? TString(tokenName) + "@" : "") + host + url;
}

TEndpointInfo PrepareEndpoint(const TString& endpoint) {
    TStringBuf scheme = "grpc";
    TStringBuf host;
    TStringBuf url;
    TStringBuf tokenName;
    CrackEndpoint(endpoint, scheme, tokenName, host, url);
    return {
        .Endpoint = TString(scheme) + "://" + host,
        .TokenName = TString(tokenName),
        .URL = TString(url),
    };
}

} // namespace NMeta
