package io.zulia.server.rest;

import io.micronaut.serde.annotation.SerdeImport;
import io.zulia.rest.dto.*;
import io.zulia.server.rest.controllers.AssociatedController;
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
@SerdeImport(StatsDTO.class)
@SerdeImport(TermDTO.class)
@SerdeImport(TermsResponseDTO.class)
public class ZuliaRESTService {

}
