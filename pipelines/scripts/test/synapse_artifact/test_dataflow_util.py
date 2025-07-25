from pipelines.scripts.synapse_artifact.synapse_dataflow_util import SynapseDataFlowUtil
from copy import deepcopy


def test__synapse_dataflow_util__compare__match():
    artifact = {
        "name": "test_dataflow",
        "properties": {
            "type": "MappingDataFlow",
            "description": "Test data flow for transformation",
            "typeProperties": {
                "sources": [
                    {
                        "dataset": {
                            "referenceName": "ds_source_customers",
                            "type": "DatasetReference"
                        },
                        "name": "sourceCustomers",
                        "description": "Customer data source"
                    },
                    {
                        "dataset": {
                            "referenceName": "ds_source_orders",
                            "type": "DatasetReference"
                        },
                        "name": "sourceOrders",
                        "description": "Order data source"
                    }
                ],
                "sinks": [
                    {
                        "dataset": {
                            "referenceName": "ds_output_customer_orders",
                            "type": "DatasetReference"
                        },
                        "name": "sinkCustomerOrders",
                        "description": "Combined customer orders output"
                    }
                ],
                "transformations": [
                    {
                        "name": "joinCustomersOrders",
                        "description": "Join customers with their orders"
                    },
                    {
                        "name": "aggregateOrderTotals",
                        "description": "Aggregate order totals by customer"
                    },
                    {
                        "name": "filterActiveCustomers",
                        "description": "Filter only active customers"
                    }
                ],
                "script": "source(output(\n\tcustomer_id as string,\n\tcustomer_name as string,\n\tstatus as string\n),\nallowSchemaDrift: true,\nvalidateSchema: false,\nisolationLevel: 'READ_UNCOMMITTED',\nformat: 'table') ~> sourceCustomers\nsource(output(\n\torder_id as string,\n\tcustomer_id as string,\n\torder_total as decimal(10,2),\n\torder_date as timestamp\n),\nallowSchemaDrift: true,\nvalidateSchema: false,\nisolationLevel: 'READ_UNCOMMITTED',\nformat: 'table') ~> sourceOrders\nsourceCustomers, sourceOrders join(sourceCustomers@customer_id == sourceOrders@customer_id,\n\tjoinType:'inner',\n\tbroadcast: 'auto')~> joinCustomersOrders\njoinCustomersOrders aggregate(groupBy(customer_id,\n\t\tcustomer_name),\n\ttotal_orders = sum(order_total)) ~> aggregateOrderTotals\naggregateOrderTotals filter(status == 'active') ~> filterActiveCustomers\nfilterActiveCustomers sink(allowSchemaDrift: true,\nvalidateSchema: false,\ninput(\n\tcustomer_id as string,\n\tcustomer_name as string,\n\ttotal_orders as decimal(10,2)\n),\ndeletable:false,\ninsertable:true,\nupdateable:false,\nupsertable:false,\nformat: 'table',\nskipDuplicateMapInputs: true,\nskipDuplicateMapOutputs: true,\nerrorHandlingOption: 'stopOnFirstError') ~> sinkCustomerOrders"
            },
            "folder": {
                "name": "customer_analytics"
            },
            "annotations": []
        },
        "type": "Microsoft.Synapse/workspaces/dataflows"
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseDataFlowUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_dataflow_util__compare__mismatch():
    artifact = {
        "name": "test_dataflow",
        "properties": {
            "type": "MappingDataFlow",
            "description": "Test data flow for transformation",
            "typeProperties": {
                "sources": [
                    {
                        "dataset": {
                            "referenceName": "ds_source_customers",
                            "type": "DatasetReference"
                        },
                        "name": "sourceCustomers",
                        "description": "Customer data source"
                    },
                    {
                        "dataset": {
                            "referenceName": "ds_source_orders",
                            "type": "DatasetReference"
                        },
                        "name": "sourceOrders",
                        "description": "Order data source"
                    }
                ],
                "sinks": [
                    {
                        "dataset": {
                            "referenceName": "ds_output_customer_orders",
                            "type": "DatasetReference"
                        },
                        "name": "sinkCustomerOrders",
                        "description": "Combined customer orders output"
                    }
                ],
                "transformations": [
                    {
                        "name": "joinCustomersOrders",
                        "description": "Join customers with their orders"
                    },
                    {
                        "name": "aggregateOrderTotals",
                        "description": "Aggregate order totals by customer"
                    },
                    {
                        "name": "filterActiveCustomers",
                        "description": "Filter only active customers"
                    }
                ],
                "script": "source(output(\n\tcustomer_id as string,\n\tcustomer_name as string,\n\tstatus as string\n),\nallowSchemaDrift: true,\nvalidateSchema: false,\nisolationLevel: 'READ_UNCOMMITTED',\nformat: 'table') ~> sourceCustomers\nsource(output(\n\torder_id as string,\n\tcustomer_id as string,\n\torder_total as decimal(10,2),\n\torder_date as timestamp\n),\nallowSchemaDrift: true,\nvalidateSchema: false,\nisolationLevel: 'READ_UNCOMMITTED',\nformat: 'table') ~> sourceOrders\nsourceCustomers, sourceOrders join(sourceCustomers@customer_id == sourceOrders@customer_id,\n\tjoinType:'inner',\n\tbroadcast: 'auto')~> joinCustomersOrders\njoinCustomersOrders aggregate(groupBy(customer_id,\n\t\tcustomer_name),\n\ttotal_orders = sum(order_total)) ~> aggregateOrderTotals\naggregateOrderTotals filter(status == 'active') ~> filterActiveCustomers\nfilterActiveCustomers sink(allowSchemaDrift: true,\nvalidateSchema: false,\ninput(\n\tcustomer_id as string,\n\tcustomer_name as string,\n\ttotal_orders as decimal(10,2)\n),\ndeletable:false,\ninsertable:true,\nupdateable:false,\nupsertable:false,\nformat: 'table',\nskipDuplicateMapInputs: true,\nskipDuplicateMapOutputs: true,\nerrorHandlingOption: 'stopOnFirstError') ~> sinkCustomerOrders"
            },
            "folder": {
                "name": "customer_analytics"
            },
            "annotations": []
        },
        "type": "Microsoft.Synapse/workspaces/dataflows"
    }
    different_attributes = {
        "properties": {
            "description": "Modified data flow description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseDataFlowUtil("some_workspace").compare(artifact, artifact_copy)
