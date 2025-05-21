# common variables loaded by default
# see https://developer.hashicorp.com/terraform/language/values/variables#variable-definitions-tfvars-files

odt_appeals_back_office_sb_topic_subscriptions = [
  {
    subscription_name = "appeal-has-odw-sub"
    topic_name        = "appeal-has"
  },
  {
    subscription_name = "appeal-s78-odw-sub"
    topic_name        = "appeal-s78"
  },
  {
    subscription_name = "appeal-document-odw-sub"
    topic_name        = "appeal-document"
  },
  {
    subscription_name = "appeal-event-odw-sub"
    topic_name        = "appeal-event"
  },
  {
    subscription_name = "appeal-service-user-odw-sub"
    topic_name        = "appeal-service-user"
  },
  {
    subscription_name = "appeal-representation-odw-sub"
    topic_name        = "appeal-representation"
  },
  {
    subscription_name = "appeal-event-estimate-odw-sub"
    topic_name        = "appeal-event-estimate"
  }
]

tooling_config = {
  network_name    = "pins-vnet-shared-tooling-uks"
  network_rg      = "pins-rg-shared-tooling-uks"
  subscription_id = "edb1ff78-90da-4901-a497-7e79f966f8e2"
}
