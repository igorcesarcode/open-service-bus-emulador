# Fixture: a Terraform file whose module comes from a private source that
# cannot be downloaded, read by the module adapter (asb_queues,
# asb_topics_name, asb_subs). Names are anonymized.
provider "azurerm" {
  version                    = "~> 4.0"
  skip_provider_registration = true
  features {}
}

terraform {
  backend "azurerm" {
    storage_account_name = "examplestate"
    container_name       = "tfstate"
    key                  = "asb/terraform.tfstate"
  }
}

module "azure-service-bus" {
  source                       = "git::https://example.invalid/private/terraform-modules/azure-service-bus.git?ref=3.0.0"
  asb_name                     = "asb-example"
  resource_group               = "rg-example"
  location_asb                 = "East US"
  asb_sku                      = "Premium"
  capacity                     = "2"
  premium_messaging_partitions = 2
  enable_queue_partitioning    = true
  asb_auth_rule                = "default_rule"
  gitlab_group_id              = "example/group"
  gitlab_variable_environments = ["PRD"]
  gitlab_token                 = var.gitlab_token
  asb_queues = [
    {
      name = "sherlog"
    }
  ]
  asb_topics_name = [
    {
      name = "orders"
    },
    {
      name = "profile-service-worker"
    },
    {
      name = "automatic-order-ingestion"
    },
    {
      name = "stores"
    },
    {
      name = "coupon"
    },
    {
      name = "ipa"
    },
    {
      name = "salesforce"
    },
    {
      name = "categorization-user"
    },
    {
      name = "customer"
    },
    {
      name = "notification"
    },
    {
      name = "payment-hub"
    },
    {
      name = "hubsell-ifood"
    },
    {
      name = "store-sync"
    },
    {
      name = "sync-campaign-progress"
    },
    {
      name = "loyalty-reports"
    },
    {
      name = "benefits"
    },
    {
      name = "promo-tech"
    },
    {
      name = "parking"
    },
    {
      name = "referral-manager"
    },
    {
      name = "allog"
    },
    {
      name = "mission"
    },
    {
      name = "wave"
    },
    {
      name = "store-insights"
    },
    {
      name = "insider-wifi-cpf"
    },
    {
      name = "sorting-hat"
    },
    {
      name = "genia-messages"
    },
    {
      name = "genia-ocr"
    },
    {
      name = "partnership"
    },
    {
      name = "order-transaction-validator"
    },
    {
      name = "eventhub"
    },
    {
      name = "partnership-member-events"
    },
    {
      name = "rag-ingest-events"
    },
    {
      name = "genia-workflow-prd"
    },
    {
      name = "espaco-lojista"
    },
    {
      name = "store-lifecycle"
    },
    {
      name = "genia-messages-ingress"
    }
  ]
  asb_subs = [
    {
      name               = "control-order"
      topic              = "orders"
      sql_filter         = "event in ('reanalyze', 'process', 'error_enrichment')"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name               = "report"
      topic              = "orders"
      sql_filter         = "event in ('report')"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name               = "order-enrichment"
      topic              = "orders"
      sql_filter         = "event in ('analyze')"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name       = "promo"
      topic      = "orders"
      sql_filter = "event in ('completed', 'approved') AND isApproved = TRUE"
    },
    {
      name       = "schedule-analyze"
      topic      = "orders"
      sql_filter = "event in ('schedule-analyze')"
    },
    {
      name       = "update_customer_promo"
      topic      = "orders"
      sql_filter = "event in ('update_customer') AND isApproved = TRUE"
    },
    {
      name       = "brmalls_lab_request"
      topic      = "automatic-order-ingestion"
      sql_filter = "event in ('brmalls_lab_request')"
    },
    {
      name               = "loyalty"
      topic              = "orders"
      sql_filter         = "(event in ('completed', 'approved') AND isApproved = TRUE) OR (event = 'completed' AND isApproved = FALSE)"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name       = "napp_request"
      topic      = "automatic-order-ingestion"
      sql_filter = "event in ('napp_request')"
    },
    {
      name       = "enrich-store"
      topic      = "stores"
      sql_filter = "command in ('EnrichStoreCommand')"
    },
    {
      name       = "update_customer_loyalty"
      topic      = "orders"
      sql_filter = "event in ('update_customer') AND isApproved = TRUE"
    },
    {
      name       = "cnpj-analysis"
      topic      = "orders"
      sql_filter = "event in ('cnpj-analysis')"
    },
    {
      name       = "backoffice"
      topic      = "stores"
      sql_filter = "event in ('emissionListItemCreated', 'emissionListItemDeleted', 'blackListItemCreated', 'blackListItemDeleted')"
    },
    {
      name       = "unknown_emission_federal_tax_id"
      topic      = "stores"
      sql_filter = "event in ('unknownEmissionFederalTaxIdFound')"
    },
    {
      name       = "burn_coupon"
      topic      = "coupon"
      sql_filter = "event in ('BURN')"
    },
    {
      name       = "store_ipa_invoices"
      topic      = "ipa"
      sql_filter = "event in ('stored_ipa_invoices')"
    },
    {
      name       = "schedule_ipa_invoices"
      topic      = "ipa"
      sql_filter = "event in ('scheduled_ipa_invoices')"
    },
    {
      name       = "create_shopping_keeper"
      topic      = "salesforce"
      sql_filter = "event in ('completed', 'approved')"
    },
    {
      name       = "categorization-user"
      topic      = "categorization-user"
      sql_filter = "event = 'CATEGORIZATION-USER'"
    },
    {
      name       = "created"
      topic      = "customer"
      sql_filter = "event in ('created') AND version = 1"
    },
    {
      name       = "send-mail-raw"
      topic      = "notification"
      sql_filter = "event in ('send_mail_raw')"
    },
    {
      name       = "send-mail-templated"
      topic      = "notification"
      sql_filter = "event in ('send_mail_templated')"
    },
    {
      name       = "send-sms"
      topic      = "notification"
      sql_filter = "event in ('send_sms')"
    },
    {
      name       = "changed-email"
      topic      = "customer"
      sql_filter = "event in ('changed-email')"
    },
    {
      name       = "benefits"
      topic      = "orders"
      sql_filter = "event in ('ingested')"
    },
    {
      name       = "order-status"
      topic      = "payment-hub"
      sql_filter = "event in ('order-paid', 'order-canceled', 'order-payment-failed')"
    },
    {
      name       = "hubsell"
      topic      = "hubsell-ifood"
      sql_filter = "command in ('GetFromHubsell') AND version = 1"
    },
    {
      name       = "ifood"
      topic      = "hubsell-ifood"
      sql_filter = "command in ('SendToIfood') AND version = 1"
    },
    {
      name       = "setup"
      topic      = "store-sync"
      sql_filter = "event in ('setup')"
    },
    {
      name       = "invoices"
      topic      = "store-sync"
      sql_filter = "event in ('invoice')"
    },
    {
      name       = "campaign-manager"
      topic      = "sync-campaign-progress"
      sql_filter = "event in ('SYNC')"
    },
    {
      name       = "benefits"
      topic      = "loyalty-reports"
      sql_filter = "event in ('benefits')"
    },
    {
      name       = "coupons"
      topic      = "loyalty-reports"
      sql_filter = "event in ('coupons')"
    },
    {
      name       = "health-check"
      topic      = "loyalty-reports"
      sql_filter = "event in ('health-check')"
    },
    {
      name       = "publish"
      topic      = "benefits"
      sql_filter = "event in ('publish')"
    },
    {
      name       = "expire"
      topic      = "benefits"
      sql_filter = "event in ('expire')"
    },
    {
      name       = "send-coupons-to-printer"
      topic      = "benefits"
      sql_filter = "event in ('send-coupons-to-printer')"
    },
    {
      name       = "health-check"
      topic      = "store-sync"
      sql_filter = "event in ('check')"
    },
    {
      name       = "customer-control-order"
      topic      = "customer"
      sql_filter = "event in ('customer-created', 'customer-deleted')"
    },
    {
      name       = "crm"
      topic      = "customer"
      sql_filter = "event in ('customer-created','customer-updated','customer-deleted', 'crm-retry','customer-in-the-mall', 'customer-referred','customer-connect-wifi','customer-leaving-mall','clube-estrela-subscription-created')"
    },
    {
      name       = "profile-migration"
      topic      = "customer"
      sql_filter = "event in ('created', 'updated', 'deleted', 'migration')"
    },
    {
      name       = "delete-customer-information"
      topic      = "customer"
      sql_filter = "event in ('customer-deleted')"
    },
    {
      name       = "crm"
      topic      = "orders"
      sql_filter = "event in ('completed', 'approved', 'crm-retry') AND isApproved = TRUE"
    },
    {
      name       = "crm"
      topic      = "coupon"
      sql_filter = "event in ('coupon_burned','coupon_created', 'coupon_achieved', 'crm-retry')"
    },
    {
      name       = "crm"
      topic      = "profile-service-worker"
      sql_filter = "command in ('UpdateLoyaltyData') OR event in ('crm-retry','update-loyalty-by-sorting-hat')"
    },
    {
      name       = "category-update-sh"
      topic      = "categorization-user"
      sql_filter = "event = 'CATEGORY-UPDATE'"
    },
    {
      name       = "ipa_invoices"
      topic      = "ipa"
      sql_filter = "event in ('new_ipa_invoices')"
    },
    {
      name       = "rescheduler"
      topic      = "promo-tech"
      sql_filter = "isReschedule = TRUE"
    },
    {
      name       = "artifacts-coupon"
      topic      = "promo-tech"
      sql_filter = "type in ('RAFFLE', 'RAFFLE_AND_BUYANDWIN') AND object = 'opt-in'"
    },
    {
      name       = "artifacts-token"
      topic      = "promo-tech"
      sql_filter = "type in ('LUCKYNUMBER', 'LUCKYNUMBER_AND_BUYANDWIN') AND object = 'opt-in'"
    },
    {
      name       = "artifacts-ticket"
      topic      = "promo-tech"
      sql_filter = "type in ('BUYANDWIN', 'LUCKYNUMBER_AND_BUYANDWIN', 'RAFFLE_AND_BUYANDWIN') AND object = 'opt-in'"
    },
    {
      name       = "send-parking-invoice"
      topic      = "payment-hub"
      sql_filter = "event in ('payment-status-paid')"
    },
    {
      name       = "benefit-burning"
      topic      = "payment-hub"
      sql_filter = "event in ('payment-status-paid') AND metadata_benefitId = TRUE"
    },
    {
      name       = "parking-ticket-update"
      topic      = "payment-hub"
      sql_filter = "event in ('payment-status-paid', 'payment-status-failed', 'payment-status-canceled') AND metadata_ticketId = TRUE AND metadata_transactionId = TRUE"
    },
    {
      name       = "ipa_canceled_invoices"
      topic      = "ipa"
      sql_filter = "event in ('new_ipa_canceled_invoices')"
    },
    {
      name       = "benefit-command-create"
      topic      = "benefits"
      sql_filter = "command in ('create')"
    },
    {
      name       = "benefit-event-created"
      topic      = "benefits"
      sql_filter = "event in ('created')"
    },
    {
      name       = "coupon-command-create"
      topic      = "coupon"
      sql_filter = "command in ('create')"
    },
    {
      name       = "coupon-event-created"
      topic      = "coupon"
      sql_filter = "event in ('created')"
    },
    {
      name       = "access-manager"
      topic      = "promo-tech"
      sql_filter = "event in ('cep-update')"
    },
    {
      name       = "redirected-include-user"
      topic      = "benefits"
      sql_filter = "command in ('redirect')"
    },
    {
      name       = "include-user"
      topic      = "benefits"
      sql_filter = "command in ('include')"
    },
    {
      name       = "crm"
      topic      = "promo-tech"
      sql_filter = "event in ('promotion_opt_in_created', 'promotion_artifact_created', 'promotion_award_redeemed', 'customer_progress_update')"
    },
    {
      name       = "store-control-order"
      topic      = "stores"
      sql_filter = "command in ('emissionCnpjStoreUpdateEvent')"
    },
    {
      name       = "customer-in-the-mall"
      topic      = "orders"
      sql_filter = "event in ('completed', 'approved') AND isApproved = TRUE"
    },
    {
      name       = "create-coupon"
      topic      = "coupon"
      sql_filter = "event in ('create')"
    },
    {
      name       = "plate-entrance"
      topic      = "parking"
      sql_filter = "event in ('parking-entrance')"
    },
    {
      name       = "customer-in-the-mall"
      topic      = "parking"
      sql_filter = "event in ('customer-in-the-mall')"
    },
    {
      name       = "benefits-ticket"
      topic      = "promo-tech"
      sql_filter = "event in ('hidden_benefit_coupon_burned')"
    },
    {
      name       = "referral-manager"
      topic      = "orders"
      sql_filter = "event in ('completed', 'approved')"
    },
    {
      name       = "crm"
      topic      = "referral-manager"
      sql_filter = "event in ('order-referral') AND isApproved = TRUE"
    },
    {
      name       = "order-paid"
      topic      = "allog"
      sql_filter = "command in ('ORDER_PAID')"
    },
    {
      name       = "mission"
      topic      = "orders"
      sql_filter = "event in ('completed', 'approved') AND isApproved = TRUE"
    },
    {
      name       = "update_customer_mission"
      topic      = "orders"
      sql_filter = "event in ('update_customer') AND isApproved = TRUE"
    },
    {
      name       = "opt-in"
      topic      = "mission"
      sql_filter = "object = 'opt-in'"
    },
    {
      name       = "notification"
      topic      = "ipa"
      sql_filter = "event in ('ipa', 'agents')"
    },
    {
      name       = "webhook-wave-connect"
      topic      = "wave"
      sql_filter = "command in ('WAVE_CONNECT')"
    },
    {
      name       = "report-created"
      topic      = "store-insights"
      sql_filter = "event in ('report-created')"
    },
    {
      name       = "crm"
      topic      = "mission"
      sql_filter = "event in ('mission_opt_in_created','mission_step_completed', 'mission_completed')"
    },
    {
      name       = "scheduled-feedback"
      topic      = "store-insights"
      sql_filter = "event in ('scheduled-feedback')"
    },
    {
      name       = "send-boleto"
      topic      = "store-insights"
      sql_filter = "event in ('send-boleto')"
    },
    {
      name       = "insider-wifi-cpf"
      topic      = "insider-wifi-cpf"
      sql_filter = "event in ('customer-connect-wifi')"
    },
    {
      name       = "check-partnership"
      topic      = "sorting-hat"
      sql_filter = "event in ('check-partnership')"
    },
    {
      name       = "sorting-hat-workers"
      topic      = "customer"
      sql_filter = "event in ('customer-created')"
    },
    {
      name       = "sorting-hat"
      topic      = "profile-service-worker"
      sql_filter = "mallCode in ('SLB') AND (command in ('UpdateLoyaltyData') OR event in ('sorting-hat-retry'))"
    },
    {
      name       = "assign-invoices"
      topic      = "promo-tech"
      sql_filter = "event in ('assign-invoices')"
    },
    {
      name       = "parking-pass"
      topic      = "parking"
      sql_filter = "event in ('parking-pass')"
    },
    {
      name       = "leaving-parking-pass"
      topic      = "parking"
      sql_filter = "event in ('leaving-parking-pass')"
    },
    {
      name       = "send-parking-pass-invoice"
      topic      = "parking"
      sql_filter = "event in ('payment-parking-pass-status-paid')"
    },
    {
      name       = "inbound-messages"
      topic      = "genia-messages"
      sql_filter = "eventVersion in ('1.0') AND direction = 'inbound'"
    },
    {
      name       = "outbound-huggy"
      topic      = "genia-messages"
      sql_filter = "eventVersion in ('1.0') AND direction = 'outbound' AND provider = 'huggy'"
    },
    {
      name       = "benefit_published_catalog"
      topic      = "coupon"
      sql_filter = "event in ('benefit_published')"
    },
    {
      name       = "send-event-invoice"
      topic      = "payment-hub"
      sql_filter = "event in ('payment-status-paid') AND metadata_eventId= TRUE"
    },
    {
      name       = "event-ticket-update"
      topic      = "payment-hub"
      sql_filter = "event in ('payment-status-paid', 'payment-status-failed', 'payment-status-canceled') AND metadata_eventId = TRUE AND metadata_transactionId = TRUE"
    },
    {
      name       = "ocr-request"
      topic      = "genia-ocr"
      sql_filter = "event in ('ocr-request')"
    },
    {
      name       = "card-deleted"
      topic      = "payment-hub"
      sql_filter = "event = 'card-deleted'"
    },
    {
      name       = "sorting-hat-workers"
      topic      = "partnership"
      sql_filter = "event in ('opt-in.approved', 'opt-out')"
    },
    {
      name       = "otv-partnership-opt-event"
      topic      = "partnership"
      sql_filter = "event in ('opt-in.approved', 'opt-out')"
    },
    {
      name       = "otv-partner-lifecycle-event"
      topic      = "partnership"
      sql_filter = "event in ('partner.created', 'partner.updated')"
    },
    {
      name       = "otv-partnership-lifecycle-event"
      topic      = "partnership"
      sql_filter = "event in ('partnership.created', 'partnership.updated', 'partnership.deactivated')"
    },
    {
      name       = "crm-dispatcher"
      topic      = "partnership"
      sql_filter = "event in ('opt-in.approved', 'opt-out')"
    },
    {
      name       = "partnership-service"
      topic      = "partnership"
      sql_filter = "event = 'opt-in.pending'"
    },
    {
      name       = "partnership"
      topic      = "payment-hub"
      sql_filter = "event in ('card-deleted')"
    },
    {
      name       = "partnership-service"
      topic      = "sorting-hat"
      sql_filter = "command = 'check-partnership'"
    },
    {
      name       = "crm"
      topic      = "parking"
      sql_filter = "event in ('parking-pass-enabled')"
    },
    {
      name       = "crm"
      topic      = "eventhub"
      sql_filter = "event in ('event-ticket-burned')"
    },
    {
      name       = "sefaz-order-enricher"
      topic      = "orders"
      sql_filter = "event in ('analyze_sefaz')"
    },
    {
      name       = "outbound-teams"
      topic      = "genia-messages"
      sql_filter = "eventVersion in ('1.0') AND direction = 'outbound' AND provider = 'teams'"
    },
    {
      name       = "inbound-status-change"
      topic      = "genia-messages"
      sql_filter = "eventVersion in ('1.0') and direction = 'inbound' and eventType = 'status-change'"
    },
    {
      name       = "accesskey-enricher"
      topic      = "orders"
      sql_filter = "event in ('analyze_accesskey')"
    },
    {
      name               = "order-transaction-validator"
      topic              = "orders"
      sql_filter         = "event in ('completed', 'approved') AND isApproved = TRUE"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name               = "control-order"
      topic              = "order-transaction-validator"
      sql_filter         = "1=1"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name       = "transaction-reprocessed"
      topic      = "order-transaction-validator"
      sql_filter = "event = 'transaction.reprocessed'"
    },
    {
      name       = "crm"
      topic      = "categorization-user"
      sql_filter = "event = 'CATEGORY-UPDATE' OR event in ('update-loyalty-by-sorting-hat')"
    },
    {
      name       = "invoice-multiplied"
      topic      = "categorization-user"
      sql_filter = "event = 'INVOICE-MULTIPLIED'"
    },
    {
      name       = "category-update-promo"
      topic      = "categorization-user"
      sql_filter = "event = 'CATEGORY-UPDATE'"
    },
    {
      name       = "partnership-document"
      topic      = "partnership-member-events"
      sql_filter = "event in ('document.removed')"
    },
    {
      name       = "partnership-document-processor"
      topic      = "partnership-member-events"
      sql_filter = "event in ('document.process.requested')"
    },
    {
      name       = "rag-processor"
      topic      = "rag-ingest-events"
      sql_filter = "event_type in ('INGESTION_REQUESTED', 'CONVERSION_REQUESTED', 'FILE_REJECTED', 'AGENT_DELETED', 'OPERATIONS_POLL_REQUESTED')"
    },
    {
      name       = "rag-processor-sub"
      topic      = "rag-ingest-events"
      sql_filter = "event_type in ('INGESTION_COMPLETED', 'CONVERSION_COMPLETED', 'DIRECTORY_IMPORT_REQUESTED', 'FILE_IMPORT_REQUESTED')"
    },
    {
      name       = "promotech"
      topic      = "partnership"
      sql_filter = "event = 'opt-in.approved'"
    },
    {
      name       = "workflow-request-prd"
      topic      = "genia-workflow-prd"
      sql_filter = "event in ('workflow-request')"
    },
    {
      name       = "genia-webchat-response"
      topic      = "genia-workflow-prd"
      sql_filter = "event in ('genia-web_chat-response')"
    },
    {
      name       = "ocr-response-order-enricher"
      topic      = "genia-ocr"
      sql_filter = "appId in ('9974d426-393d-466b-802d-f12fb85d0f9f') AND event in ('response')"
    },
    {
      name       = "invoice-status"
      topic      = "payment-hub"
      sql_filter = "event in ('invoice-paid', 'invoice-canceled', 'invoice-payment-failed')"
    },
    {
      name       = "publish-circular"
      topic      = "espaco-lojista"
      sql_filter = "event in ('publish-circular')"
    },
    {
      name       = "complete-service-order"
      topic      = "espaco-lojista"
      sql_filter = "event in ('complete-service-order')"
    },
    {
      name       = "bank-slip-sync-mall"
      topic      = "espaco-lojista"
      sql_filter = "event in ('bank-slip.sync-mall')"
    },
    {
      name       = "bank-slip-process-header"
      topic      = "espaco-lojista"
      sql_filter = "event in ('bank-slip.process-header')"
    },
    {
      name       = "alert-scheduled"
      topic      = "espaco-lojista"
      sql_filter = "event in ('alert-scheduled')"
    },
    {
      name               = "ocr-enricher-bootstrap"
      topic              = "orders"
      sql_filter         = "event = 'analyze_ocr' AND enrichmentProvider = 'OCR'"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name               = "ocr-enricher-after-genia"
      topic              = "orders"
      sql_filter         = "event = 'analyze_ocr' AND enrichmentProvider = 'GENIA'"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name               = "ocr-enricher-after-cognitive"
      topic              = "orders"
      sql_filter         = "event = 'analyze_ocr' AND enrichmentProvider = 'COGNITIVE'"
      lock_duration      = "PT30S"
      max_delivery_count = 3
    },
    {
      name       = "store-lifecycle-control-order"
      topic      = "store-lifecycle"
      sql_filter = "1=1"
    },
    {
      name       = "loyalty-multiplier"
      topic      = "stores"
      sql_filter = "1=1"
    },
    {
      name               = "status-change-enrichment"
      topic              = "genia-messages-ingress"
      sql_filter         = "eventVersion = '1.0' AND eventType = 'status-change.raw'"
      lock_duration      = "PT5M"
      max_delivery_count = 2
    }
  ]


  tags = {
    environment  = "prd"
    subscription = "example"
    projeto      = "asb-example"
    bu           = "b2c"
    subdominio   = "Infra"
    terraform    = "true"
  }
}
