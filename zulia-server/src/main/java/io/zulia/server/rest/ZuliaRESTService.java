package io.zulia.server.rest;

import io.micronaut.serde.annotation.SerdeImport;
import io.zulia.message.ZuliaBase;
import io.zulia.rest.dto.*;
import io.zulia.server.rest.controllers.AssociatedController;
import io.zulia.server.serde.ShardCacheStatsModifier;
import io.zulia.server.serde.IndexStatsModifier;
import io.zulia.server.serde.NodeStatsModifier;
import io.zulia.server.serde.ProtoIgnoredGetters;
import jakarta.inject.Singleton;

@Singleton
@SerdeImport(AnalysisDTO.class)
@SerdeImport(AnalysisResultDTO.class)
@SerdeImport(AssociatedController.Filenames.class)
@SerdeImport(AssociatedMetadataDTO.class)
@SerdeImport(FacetDTO.class)
@SerdeImport(FacetsDTO.class)
@SerdeImport(FieldsDTO.class)
@SerdeImport(HighlightDTO.class)
@SerdeImport(IndexesResponseDTO.class)
@SerdeImport(IndexMappingDTO.class)
@SerdeImport(NodeDTO.class)
@SerdeImport(NodesResponseDTO.class)
@SerdeImport(ScoredResultDTO.class)
@SerdeImport(SearchResultsDTO.class)
@SerdeImport(value = ZuliaBase.NodeStats.class, mixin = NodeStatsModifier.class)
@SerdeImport(value = ZuliaBase.NodeStats.class, mixin = ProtoIgnoredGetters.class)
@SerdeImport(value = ZuliaBase.IndexStats.class, mixin = IndexStatsModifier.class)
@SerdeImport(value = ZuliaBase.IndexStats.class, mixin = ProtoIgnoredGetters.class)
@SerdeImport(value = ZuliaBase.ShardCacheStats.class, mixin = ShardCacheStatsModifier.class)
@SerdeImport(value = ZuliaBase.ShardCacheStats.class, mixin = ProtoIgnoredGetters.class)
@SerdeImport(value = ZuliaBase.CacheStats.class, mixin = ProtoIgnoredGetters.class)
@SerdeImport(TermDTO.class)
@SerdeImport(TermsResponseDTO.class)
public class ZuliaRESTService {

}
