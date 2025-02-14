namespace NMeta {

class TMetaDatabase {

    TString static GetMetaDatabaseAuthToken(const TRequest& request);
    NYdb::NTable::TClientSettings static GetClientSettings(const TRequest& request, const TYdbLocation& location);
};

} // namespace NMeta
