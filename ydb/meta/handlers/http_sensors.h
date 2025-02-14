#pragma once
#include <ydb/meta/meta.h>
#include <library/cpp/monlib/encode/json/json.h>

namespace NMeta {

class THandlerActorHttpSensors : public NActors::TActor<THandlerActorHttpSensors> {
public:
    using TBase = NActors::TActor<THandlerActorHttpSensors>;

    THandlerActorHttpSensors()
        : TBase(&THandlerActorHttpSensors::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        TMetaAppData* appData = MetaAppData();
        TStringStream out;
        NMonitoring::IMetricEncoderPtr encoder = NMonitoring::EncoderJson(&out);
        appData->MetricRegistry->Accept(TInstant::Zero(), encoder.Get());
        auto response = event->Get()->Request->CreateResponseOK(out.Str(), "application/json");
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta
