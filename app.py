#!/usr/bin/env python3

import aws_cdk as cdk

from cruk_cdk.cruk_cdk_stack import CrukCdkStack


app = cdk.App()
CrukCdkStack(app, "cruk-cdk")

app.synth()
