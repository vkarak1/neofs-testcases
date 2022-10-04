import os
from datetime import datetime, timedelta

import allure
import pytest
from python_keywords.storage_policy import get_simple_object_copies
from python_keywords.utility_keywords import generate_file
from s3_helper import check_objects_in_bucket, object_key_from_file_path, set_bucket_versioning

from steps import s3_gate_bucket, s3_gate_object
from steps.s3_gate_base import TestS3GateBase


def pytest_generate_tests(metafunc):
    if "s3_client" in metafunc.fixturenames:
        metafunc.parametrize("s3_client", ["aws cli", "boto3"], indirect=True)


@pytest.mark.s3_gate
class TestS3GateBucket(TestS3GateBase):
    @allure.title("Test S3: Create Bucket with different ACL")
    def test_s3_create_bucket_with_ACL(self):

        with allure.step("Create bucket with ACL private"):
            bucket = s3_gate_bucket.create_bucket_s3(self.s3_client, True, acl="private")
            bucket_acl = s3_gate_bucket.get_bucket_acl(self.s3_client, bucket)
            bucket_permission = [permission.get("Permission") for permission in bucket_acl]
            assert bucket_permission == [
                "FULL_CONTROL"
            ], "Permission for CanonicalUser is FULL_CONTROL"

        with allure.step("Create bucket with ACL = public-read"):
            bucket_1 = s3_gate_bucket.create_bucket_s3(self.s3_client, True, acl="public-read")
            bucket_acl_1 = s3_gate_bucket.get_bucket_acl(self.s3_client, bucket_1)
            bucket_permission_1 = [permission.get("Permission") for permission in bucket_acl_1]
            assert bucket_permission_1 == [
                "FULL_CONTROL",
                "FULL_CONTROL",
            ], "Permission for all groups is FULL_CONTROL"

        with allure.step("Create bucket with ACL public-read-write"):
            bucket_2 = s3_gate_bucket.create_bucket_s3(
                self.s3_client, True, acl="public-read-write"
            )
            bucket_acl_2 = s3_gate_bucket.get_bucket_acl(self.s3_client, bucket_2)
            bucket_permission_2 = [permission.get("Permission") for permission in bucket_acl_2]
            assert bucket_permission_2 == [
                "FULL_CONTROL",
                "FULL_CONTROL",
            ], "Permission for CanonicalUser is FULL_CONTROL"

        with allure.step("Create bucket with ACL = authenticated-read"):
            bucket_3 = s3_gate_bucket.create_bucket_s3(
                self.s3_client, True, acl="authenticated-read"
            )
            bucket_acl_3 = s3_gate_bucket.get_bucket_acl(self.s3_client, bucket_3)
            bucket_permission_3 = [permission.get("Permission") for permission in bucket_acl_3]
            assert bucket_permission_3 == [
                "FULL_CONTROL",
                "FULL_CONTROL",
            ], "Permission for all groups is FULL_CONTROL"

    @allure.title("Test S3: Create Bucket with different ACL by grand")
    def test_s3_create_bucket_with_grands(self):

        with allure.step("Create bucket with  --grant-read"):
            bucket = s3_gate_bucket.create_bucket_s3(
                self.s3_client,
                True,
                grant_read="uri=http://acs.amazonaws.com/groups/global/AllUsers",
            )
            bucket_acl = s3_gate_bucket.get_bucket_acl(self.s3_client, bucket)
            bucket_permission = [permission.get("Permission") for permission in bucket_acl]
            assert bucket_permission == [
                "FULL_CONTROL",
                "FULL_CONTROL",
            ], "Permission for CanonicalUser is FULL_CONTROL"

        with allure.step("Create bucket with --grant-wtite"):
            bucket_1 = s3_gate_bucket.create_bucket_s3(
                self.s3_client,
                True,
                grant_write="uri=http://acs.amazonaws.com/groups/global/AllUsers",
            )
            bucket_acl_1 = s3_gate_bucket.get_bucket_acl(self.s3_client, bucket_1)
            bucket_permission_1 = [permission.get("Permission") for permission in bucket_acl_1]
            assert bucket_permission_1 == [
                "FULL_CONTROL",
                "FULL_CONTROL",
            ], "Permission for all groups is FULL_CONTROL"

        with allure.step("Create bucket with --grant-full-control"):
            bucket_2 = s3_gate_bucket.create_bucket_s3(
                self.s3_client,
                True,
                grant_full_control="uri=http://acs.amazonaws.com/groups/global/AllUsers",
            )
            bucket_acl_2 = s3_gate_bucket.get_bucket_acl(self.s3_client, bucket_2)
            bucket_permission_2 = [permission.get("Permission") for permission in bucket_acl_2]
            assert bucket_permission_2 == [
                "FULL_CONTROL",
                "FULL_CONTROL",
            ], "Permission for CanonicalUser is FULL_CONTROL"

    @allure.title("Test S3: create bucket with object lock")
    def test_s3_bucket_object_lock(self):
        file_path = generate_file()
        file_name = object_key_from_file_path(file_path)

        with allure.step("Create bucket with --no-object-lock-enabled-for-bucket"):
            bucket = s3_gate_bucket.create_bucket_s3(self.s3_client, False)
            date_obj = datetime.utcnow() + timedelta(days=1)
            with pytest.raises(
                Exception, match=r".*Object Lock configuration does not exist for this bucket.*"
            ):
                # An error occurred (ObjectLockConfigurationNotFoundError) when calling the PutObject operation (reached max retries: 0):
                # Object Lock configuration does not exist for this bucket
                s3_gate_object.put_object_s3(
                    self.s3_client,
                    bucket,
                    file_path,
                    ObjectLockMode="COMPLIANCE",
                    ObjectLockRetainUntilDate=date_obj.strftime("%Y-%m-%dT%H:%M:%S"),
                )
        with allure.step("Create bucket with --object-lock-enabled-for-bucket"):
            bucket_1 = s3_gate_bucket.create_bucket_s3(self.s3_client, True)
            date_obj_1 = datetime.utcnow() + timedelta(days=1)
            s3_gate_object.put_object_s3(
                self.s3_client,
                bucket_1,
                file_path,
                ObjectLockMode="COMPLIANCE",
                ObjectLockRetainUntilDate=date_obj_1.strftime("%Y-%m-%dT%H:%M:%S"),
                ObjectLockLegalHoldStatus="ON",
            )
            object_1 = s3_gate_object.get_object_s3(
                self.s3_client, bucket_1, file_name, full_output=True
            )
            assert (
                object_1.get("ObjectLockMode") == "COMPLIANCE"
            ), "Expected Object Lock Mode is COMPLIANCE"
            assert str(date_obj_1.strftime("%Y-%m-%dT%H:%M:%S")) in object_1.get(
                "ObjectLockRetainUntilDate"
            ), f'Expected Object Lock Retain Until Date is {str(date_obj_1.strftime("%Y-%m-%dT%H:%M:%S"))}'
            assert (
                object_1.get("ObjectLockLegalHoldStatus") == "ON"
            ), "Expected Object Lock Legal Hold Status is ON"

    @allure.title("Test S3: delete bucket")
    def test_s3_delete_bucket(self):
        file_path_1 = generate_file()
        file_name_1 = object_key_from_file_path(file_path_1)
        file_path_2 = generate_file()
        file_name_2 = object_key_from_file_path(file_path_2)
        bucket = s3_gate_bucket.create_bucket_s3(self.s3_client)

        with allure.step("Put two objects into bucket"):
            s3_gate_object.put_object_s3(self.s3_client, bucket, file_path_1)
            s3_gate_object.put_object_s3(self.s3_client, bucket, file_path_2)
            check_objects_in_bucket(self.s3_client, bucket, [file_name_1, file_name_2])

        with allure.step("Try to delete not empty bucket and get error"):
            with pytest.raises(Exception, match=r".*The bucket you tried to delete is not empty.*"):
                s3_gate_bucket.delete_bucket_s3(self.s3_client, bucket)

        with allure.step("Delete object in bucket"):
            s3_gate_object.delete_object_s3(self.s3_client, bucket, file_name_1)
            s3_gate_object.delete_object_s3(self.s3_client, bucket, file_name_2)
            check_objects_in_bucket(self.s3_client, bucket, [])

        with allure.step(f"Delete empty bucket"):
            s3_gate_bucket.delete_bucket_s3(self.s3_client, bucket)
            with pytest.raises(Exception, match=r".*Not Found.*"):
                s3_gate_bucket.head_bucket(self.s3_client, bucket)
