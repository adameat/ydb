#pragma once
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>

namespace NMeta {
    class TMetaTokenator;
}

struct TMetaAppData {
    std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry;
    NMeta::TMetaTokenator* Tokenator = nullptr;
};

inline TMetaAppData* MetaAppData() {
    return NActors::TActivationContext::ActorSystem()->template AppData<TMetaAppData>();
}
