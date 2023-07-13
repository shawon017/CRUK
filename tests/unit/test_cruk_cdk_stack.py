import aws_cdk as core
import aws_cdk.assertions as assertions
from cruk_cdk.cruk_cdk_stack import CrukCdkStack

"""
    Just a simple test to check that the stack can be deployed
    and that the bucket is created with the correct properties.
"""

def stack():
    app = core.App()
    return CrukCdkStack(app, "myteststack")

def test_bronze_bucket(stack):
    bucket = stack.node.try_find_child("BronzeBucket")
    assert bucket is not None
    assert bucket.versioned == True
    assert bucket.public_read_access == False

def test_silver_bucket(stack):
    bucket = stack.node.try_find_child("SilverBucket")
    assert bucket is not None
    assert bucket.versioned == True
    assert bucket.public_read_access == False

def test_gold_bucket(stack):
    bucket = stack.node.try_find_child("GoldBucket")
    assert bucket is not None
    assert bucket.versioned == True
    assert bucket.public_read_access == False

def test_placeholder_bucket(stack):
    bucket = stack.node.try_find_child("PlaceholderBucket")
    assert bucket is not None
    assert bucket.versioned == True
    assert bucket.public_read_access == False

# def test_sqs_queue_created():
#     app = core.App()
#     stack = CrukCdkStack(app, "cruk-cdk")
#     template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })


# def test_sns_topic_created():
#     app = core.App()
#     stack = CrukCdkStack(app, "cruk-cdk")
#     template = assertions.Template.from_stack(stack)

#     template.resource_count_is("AWS::SNS::Topic", 1)
