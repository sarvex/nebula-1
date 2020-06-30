/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_MOCKDATA_H_
#define MOCK_MOCKDATA_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/meta/NebulaSchemaProvider.h"

DECLARE_bool(mock_ttl_col);
DECLARE_int32(mock_ttl_duration);

namespace nebula {
namespace mock {

struct VertexData {
    VertexID            vId_;
    TagID               tId_;
    std::vector<Value>  props_;
};

struct EdgeData {
    VertexID            srcId_;
    EdgeType            type_;
    EdgeRanking         rank_;
    VertexID            dstId_;
    std::vector<Value>  props_;
};

struct Player {
    std::string         name_;
    int                 age_;
    bool                playing_;
    int                 career_;
    int                 startYear_;
    int                 endYear_;
    int                 games_;
    double              avgScore_;
    int                 serveTeams_;
    std::string         country_{""};
    int                 champions_{0};
};

struct Serve {
    std::string         playerName_;
    std::string         teamName_;
    int                 startYear_;
    int                 endYear_;
    int                 teamCareer_;
    int                 teamGames_;
    double              teamAvgScore_;
    std::string         type_;
    int                 champions_{0};
};

struct Teammate {
    std::string         player1_;
    std::string         player2_;
    std::string         teamName_;
    int                 startYear_;
    int                 endYear_;
};

class MockData {
public:
    /*
     * Mock schema
     */
    static std::shared_ptr<meta::NebulaSchemaProvider> mockPlayerTagSchema(SchemaVer ver = 0);

    static std::shared_ptr<meta::NebulaSchemaProvider> mockTeamTagSchema(SchemaVer ver = 0);

    static std::shared_ptr<meta::NebulaSchemaProvider> mockServeSchema(SchemaVer ver = 0);

    static std::shared_ptr<meta::NebulaSchemaProvider> mockTeammateSchema(SchemaVer ver = 0);

    static std::vector<nebula::meta::cpp2::ColumnDef> mockGeneralTagIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockSimpleTagIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockEdgeIndexColumns();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockGeneralTagSchemaV1();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockGeneralTagSchemaV2();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockTypicaSchemaV2();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockTypicaIndexColumns();

    /*
     * Mock data
     */
    // Construct data in the order of schema
    // generate player and team tag
    static std::vector<VertexData> mockVertices();

    // generate serve edge
    static std::vector<EdgeData> mockEdges();

    // generate serve and teammate edge
    static std::vector<EdgeData> mockMultiEdges();

    static std::vector<VertexID> mockVerticeIds();

    // generate serve edge with different rank
    static std::unordered_map<VertexID, std::vector<EdgeData>> mockmMultiRankServes(
            EdgeRanking rankCount = 1);

    // generate player -> list<Serve> according to players_;
    static std::unordered_map<std::string, std::vector<Serve>> playerServes() {
        std::unordered_map<std::string, std::vector<Serve>> result;
        for (const auto& serve : serves_) {
            result[serve.playerName_].emplace_back(serve);
        }
        return result;
    }

    // generate team -> list<Serve> according to serves_;
    static std::unordered_map<std::string, std::vector<Serve>> teamServes() {
        std::unordered_map<std::string, std::vector<Serve>> result;
        for (const auto& serve : serves_) {
            result[serve.teamName_].emplace_back(serve);
        }
        return result;
    }

    static nebula::storage::cpp2::AddVerticesRequest mockAddVertices(int32_t parts = 6);

    // Only has EdgeKey data, not props
    static std::vector<EdgeData> mockEdgeKeys();

    // Construct data in the specified order
    // For convenience, here is the reverse order
    static std::vector<VertexData> mockVerticesSpecifiedOrder();

    static std::vector<EdgeData> mockEdgesSpecifiedOrder();

    /*
     * Mock request
     */
    static nebula::storage::cpp2::AddVerticesRequest
    mockAddVerticesReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest
    mockAddEdgesReq(int32_t parts = 6);

    static nebula::storage::cpp2::DeleteVerticesRequest
    mockDeleteVerticesReq(int32_t parts = 6);

    static nebula::storage::cpp2::DeleteEdgesRequest
    mockDeleteEdgesReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddVerticesRequest
    mockAddVerticesSpecifiedOrderReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest
    mockAddEdgesSpecifiedOrderReq(int32_t parts = 6);

    /*
     * Mock KV data
     */
    static nebula::storage::cpp2::KVPutRequest mockKVPut();

    static nebula::storage::cpp2::KVGetRequest mockKVGet();

    static nebula::storage::cpp2::KVRemoveRequest mockKVRemove();

public:
    static std::vector<std::string> teams_;

    static std::vector<Player> players_;

    static std::vector<Serve> serves_;

    static std::vector<Teammate> teammates_;

    // player name -> list<Serve>
    static std::unordered_map<std::string, std::vector<Serve>> playerServes_;

    // team name -> list<Serve>
    static std::unordered_map<std::string, std::vector<Serve>> teamServes_;

    static EdgeData getReverseEdge(const EdgeData& edge);
};

}  // namespace mock
}  // namespace nebula

#endif  // MOCK_MOCKDATA_H_