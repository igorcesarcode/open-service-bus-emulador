# Example production Terraform for Azure Service Bus.
#
# open-service-bus reads this file and builds the same namespace, queues,
# topics, subscriptions and rules locally. Every attribute below must be
# honored by the emulator; attributes that only make sense in Azure
# (location, sku, tags, network settings) are parsed and ignored.

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "environment" {
  type    = string
  default = "prd"
}

locals {
  prefix = "asb-shop-${var.environment}"

  # Topics that share the same settings, created with for_each.
  simple_topics = toset(["customer", "notification", "payment-hub"])
}

resource "azurerm_resource_group" "main" {
  name     = "rg-shop-${var.environment}"
  location = "East US"
}

resource "azurerm_servicebus_namespace" "main" {
  name                          = local.prefix
  location                      = azurerm_resource_group.main.location
  resource_group_name           = azurerm_resource_group.main.name
  sku                           = "Premium"
  capacity                      = 1
  premium_messaging_partitions  = 1
  local_auth_enabled            = true
  public_network_access_enabled = true
  minimum_tls_version           = "1.2"

  tags = {
    environment = var.environment
  }
}

resource "azurerm_servicebus_namespace_authorization_rule" "app" {
  name         = "app"
  namespace_id = azurerm_servicebus_namespace.main.id
  listen       = true
  send         = true
  manage       = false
}

# ---------------------------------------------------------------------------
# Queues
# ---------------------------------------------------------------------------

# A queue with every property set explicitly.
resource "azurerm_servicebus_queue" "orders_inbox" {
  name         = "orders-inbox"
  namespace_id = azurerm_servicebus_namespace.main.id

  lock_duration                           = "PT2M"
  max_delivery_count                      = 5
  default_message_ttl                     = "P14D"
  auto_delete_on_idle                     = "P10675199DT2H48M5.4775807S"
  dead_lettering_on_message_expiration    = true
  requires_duplicate_detection            = true
  duplicate_detection_history_time_window = "PT10M"
  requires_session                        = false
  batched_operations_enabled              = true
  partitioning_enabled                    = false
  express_enabled                         = false
  max_size_in_megabytes                   = 5120
  max_message_size_in_kilobytes           = 1024
  status                                  = "Active"
  forward_dead_lettered_messages_to       = azurerm_servicebus_queue.orders_poison.name
}

resource "azurerm_servicebus_queue" "orders_poison" {
  name                = "orders-poison"
  namespace_id        = azurerm_servicebus_namespace.main.id
  default_message_ttl = "P30D"
}

# A session-enabled queue.
resource "azurerm_servicebus_queue" "chat" {
  name               = "chat-sessions"
  namespace_id       = azurerm_servicebus_namespace.main.id
  requires_session   = true
  lock_duration      = "PT30S"
  max_delivery_count = 10
}

# A queue that forwards everything it receives to another entity.
resource "azurerm_servicebus_queue" "legacy_orders" {
  name         = "legacy-orders"
  namespace_id = azurerm_servicebus_namespace.main.id
  forward_to   = azurerm_servicebus_topic.orders.name
}

# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------

resource "azurerm_servicebus_topic" "orders" {
  name         = "orders"
  namespace_id = azurerm_servicebus_namespace.main.id

  default_message_ttl                     = "P7D"
  auto_delete_on_idle                     = "P10675199DT2H48M5.4775807S"
  requires_duplicate_detection            = true
  duplicate_detection_history_time_window = "PT5M"
  batched_operations_enabled              = true
  partitioning_enabled                    = false
  express_enabled                         = false
  support_ordering                        = true
  max_size_in_megabytes                   = 5120
  max_message_size_in_kilobytes           = 1024
  status                                  = "Active"
}

resource "azurerm_servicebus_topic" "simple" {
  for_each     = local.simple_topics
  name         = each.value
  namespace_id = azurerm_servicebus_namespace.main.id
}

# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

# A subscription with every property set explicitly.
resource "azurerm_servicebus_subscription" "orders_enrichment" {
  name     = "order-enrichment"
  topic_id = azurerm_servicebus_topic.orders.id

  max_delivery_count                        = 3
  lock_duration                             = "PT30S"
  default_message_ttl                       = "P2D"
  auto_delete_on_idle                       = "P10675199DT2H48M5.4775807S"
  dead_lettering_on_message_expiration      = true
  dead_lettering_on_filter_evaluation_error = true
  batched_operations_enabled                = true
  requires_session                          = false
  status                                    = "Active"
  forward_dead_lettered_messages_to         = azurerm_servicebus_queue.orders_poison.name
}

resource "azurerm_servicebus_subscription" "orders_report" {
  name               = "report"
  topic_id           = azurerm_servicebus_topic.orders.id
  max_delivery_count = 3
}

resource "azurerm_servicebus_subscription" "orders_audit" {
  name               = "audit"
  topic_id           = azurerm_servicebus_topic.orders.id
  max_delivery_count = 1
  forward_to         = azurerm_servicebus_queue.orders_inbox.name
}

resource "azurerm_servicebus_subscription" "customer_crm" {
  name               = "crm"
  topic_id           = azurerm_servicebus_topic.simple["customer"].id
  max_delivery_count = 3
  requires_session   = true
}

resource "azurerm_servicebus_subscription" "notification_sms" {
  name               = "send-sms"
  topic_id           = azurerm_servicebus_topic.simple["notification"].id
  max_delivery_count = 3
}

# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

# A SQL filter with an action that rewrites properties.
resource "azurerm_servicebus_subscription_rule" "orders_enrichment_sql" {
  name            = "analyze"
  subscription_id = azurerm_servicebus_subscription.orders_enrichment.id
  filter_type     = "SqlFilter"
  sql_filter      = "event IN ('analyze', 'reanalyze') AND isApproved = TRUE"
  action          = "SET stage = 'enrichment'; REMOVE debug"
}

resource "azurerm_servicebus_subscription_rule" "orders_report_sql" {
  name            = "report"
  subscription_id = azurerm_servicebus_subscription.orders_report.id
  filter_type     = "SqlFilter"
  sql_filter      = "event = 'report' OR (event = 'completed' AND isApproved = FALSE)"
}

# A correlation filter using every system property and custom properties.
resource "azurerm_servicebus_subscription_rule" "customer_crm_correlation" {
  name            = "crm-events"
  subscription_id = azurerm_servicebus_subscription.customer_crm.id
  filter_type     = "CorrelationFilter"

  correlation_filter {
    content_type        = "application/json"
    correlation_id      = "crm"
    label               = "customer-updated"
    message_id          = "fixed-message-id"
    reply_to            = "crm-replies"
    reply_to_session_id = "crm-session"
    session_id          = "tenant-1"
    to                  = "crm-service"

    properties = {
      source  = "profile-service"
      version = "1"
    }
  }
}

resource "azurerm_servicebus_subscription_rule" "notification_sms_sql" {
  name            = "sms-only"
  subscription_id = azurerm_servicebus_subscription.notification_sms.id
  filter_type     = "SqlFilter"
  sql_filter      = "event = 'send_sms'"
}
