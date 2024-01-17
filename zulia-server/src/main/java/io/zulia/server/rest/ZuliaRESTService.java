package io.zulia.server.rest;

import io.micronaut.serde.annotation.SerdeImport;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.rest.dto.FieldsDTO;
import io.zulia.rest.dto.IndexMappingDTO;
import io.zulia.rest.dto.IndexesResponseDTO;
import io.zulia.rest.dto.NodeDTO;
import io.zulia.rest.dto.NodesResponseDTO;
import io.zulia.rest.dto.StatsDTO;
import io.zulia.rest.dto.TermDTO;
import io.zulia.rest.dto.TermsResponseDTO;
import io.zulia.server.rest.controllers.AssociatedController;
import jakarta.inject.Singleton;

@Singleton
@SerdeImport(IndexesResponseDTO.class)
@SerdeImport(NodesResponseDTO.class)
@SerdeImport(NodeDTO.class)
@SerdeImport(IndexMappingDTO.class)
@SerdeImport(FieldsDTO.class)
@SerdeImport(StatsDTO.class)
@SerdeImport(TermDTO.class)
@SerdeImport(TermsResponseDTO.class)
@SerdeImport(AssociatedController.Filenames.class)
@SerdeImport(AssociatedMetadataDTO.class)
public class ZuliaRESTService {

}
