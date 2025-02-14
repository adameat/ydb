
TString TMVP::GetMetaDatabaseAuthToken(const TRequest& request) {
    TString authToken;
    if (TMVP::MetaDatabaseTokenName.empty()) {
        authToken = request.GetAuthToken();
    } else {
        NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
        if (tokenator) {
            authToken = tokenator->GetToken(TMVP::MetaDatabaseTokenName);
        }
    }
    return authToken;
}

NYdb::NTable::TClientSettings TMVP::GetMetaDatabaseClientSettings(const TRequest& request, const TYdbLocation& location) {
    NYdb::NTable::TClientSettings clientSettings;
    clientSettings.AuthToken(GetMetaDatabaseAuthToken(request));
    clientSettings.Database(location.RootDomain);
    if (TString database = location.GetDatabaseName(request)) {
        clientSettings.Database(database);
    }
    return clientSettings;
}

