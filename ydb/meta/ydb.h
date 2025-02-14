#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <library/cpp/json/json_value.h>

namespace NMeta {

template <typename T>
class TAtomicSingleton {
private:
    mutable TAtomic Pointer;

public:
    TAtomicSingleton()
        : Pointer(0)
    {}

    ~TAtomicSingleton() {
        delete reinterpret_cast<T*>(Pointer);
    }

    T& GetRef(std::function<T*()> init = [](){ return new T(); }) const {
        if (Pointer == 0) {
            T* newValue = init();
            if (!AtomicCas(&Pointer, reinterpret_cast<TAtomic>(newValue), 0)) {
                delete newValue;
            }
        }
        return *reinterpret_cast<T*>(Pointer);
    }
};

class TYdbLocation {
public:
    TString Endpoint;

    TYdbLocation(const TString& endpoint);
    NYdb::NQuery::TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const NYdb::TParams& params = NYdb::TParamsBuilder().Build(), const NYdb::NQuery::TExecuteQuerySettings& settings = {});
    static NJson::TJsonValue ConvertResult(const NYdb::NQuery::TExecuteQueryResult& result);

private:
    static TString ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser);
    static TString ColumnValueToString(const NYdb::TValue& value);
    static TString ColumnValueToString(NYdb::TValueParser& valueParser);
    static NJson::TJsonValue ColumnPrimitiveValueToJsonValue(NYdb::TValueParser& valueParser);
    static NJson::TJsonValue ColumnValueToJsonValue(NYdb::TValueParser& valueParser);
    NYdb::TDriverConfig GetDriverConfig();
    NYdb::TDriver& GetDriver();

    TAtomicSingleton<NYdb::TDriver> Driver;
};

bool CrackEndpoint(TStringBuf endpoint, TStringBuf& scheme, TStringBuf& tokenName, TStringBuf& host, TStringBuf& uri);
TString ConstructEndpoint(const TString& endpoint, const TString& tokenName = {});

struct TEndpointInfo {
    TString Endpoint;
    TString TokenName;
    TString URL;
};

TEndpointInfo PrepareEndpoint(const TString& endpoint);

} // namespace NMeta
