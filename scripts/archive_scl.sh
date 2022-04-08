#!/bin/bash

# `earthengine authenticate` before running

date=$1
archivedir_poly=projects/SCL/v1/Panthera_tigris/canonical/pothab_archive
archivedir_pothab=projects/SCL/v1/Panthera_tigris/canonical/pothab/potential_habitat_archive
archivedir_sclimage=projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_image_archive
archivedir_obs_adhoc=projects/SCL/v1/Panthera_tigris/canonical/obs_archive/adhoc
archivedir_obs_ct=projects/SCL/v1/Panthera_tigris/canonical/obs_archive/ct
archivedir_obs_ss=projects/SCL/v1/Panthera_tigris/canonical/obs_archive/ss

earthengine rm "${archivedir_pothab}"/potential_habitat_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/potential_habitat/potential_habitat_"${date}" "${archivedir_pothab}"/potential_habitat_"${date}"
earthengine rm "${archivedir_sclimage}"/scl_image_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_image/scl_image_"${date}" "${archivedir_sclimage}"/scl_image_"${date}"

earthengine rm "${archivedir_poly}"/scl_polys_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_polys/scl_polys_"${date}" "${archivedir_poly}"/scl_polys_"${date}"
earthengine rm "${archivedir_poly}"/scl_restoration_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_restoration/scl_restoration_"${date}" "${archivedir_poly}"/scl_restoration_"${date}"
earthengine rm "${archivedir_poly}"/scl_restoration_fragment_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_restoration_fragment/scl_restoration_fragment_"${date}" "${archivedir_poly}"/scl_restoration_fragment_"${date}"
earthengine rm "${archivedir_poly}"/scl_species_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_species/scl_species_"${date}" "${archivedir_poly}"/scl_species_"${date}"
earthengine rm "${archivedir_poly}"/scl_species_fragment_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_species_fragment/scl_species_fragment_"${date}" "${archivedir_poly}"/scl_species_fragment_"${date}"
earthengine rm "${archivedir_poly}"/scl_survey_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_survey/scl_survey_"${date}" "${archivedir_poly}"/scl_survey_"${date}"
earthengine rm "${archivedir_poly}"/scl_survey_fragment_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_survey_fragment/scl_survey_fragment_"${date}" "${archivedir_poly}"/scl_survey_fragment_"${date}"
earthengine rm "${archivedir_poly}"/scl_scored_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/pothab/scl_scored/scl_scored_"${date}" "${archivedir_poly}"/scl_scored_"${date}"

earthengine rm "${archivedir_obs_adhoc}"/adhoc_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/obs/adhoc/adhoc_"${date}" "${archivedir_obs_adhoc}"/adhoc_"${date}"
earthengine rm "${archivedir_obs_ct}"/ct_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/obs/ct/ct_"${date}" "${archivedir_obs_ct}"/ct_"${date}"
earthengine rm "${archivedir_obs_ss}"/ss_"${date}"
earthengine mv projects/SCL/v1/Panthera_tigris/canonical/obs/ss/ss_"${date}" "${archivedir_obs_ss}"/ss_"${date}"
