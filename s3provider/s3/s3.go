package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rancher/rke-tools/types"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsCredentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	s3ServerRetries = 3
)

var s3Session *session.Session

type StorageProvider struct {
}

func NewStorageProvider(bc *types.BackupOpts) (types.StorageProvider, error) {
	var err error
	s3Session, err = newSession(bc, true)
	if err != nil {
		return &StorageProvider{}, err
	}
	return &StorageProvider{}, nil
}

// Upload implements storage driver upload interface
func (s StorageProvider) Upload(bc *types.BackupOpts, filePath string) error {
	log.Infof("Invoking uploading backup file %s to %s", bc.Name, bc.Provider)
	uploader := s3manager.NewUploader(s3Session)
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		log.Errorf("failed to open backup file %s, %v", bc.Name, err)
		return fmt.Errorf("failed to open backup file %s, %v", bc.Name, err)
	}

	for retries := 0; retries < s3ServerRetries; retries++ {
		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bc.BucketName),
			Key:    aws.String(bc.Name),
			Body:   file,
		})
		if err != nil {
			log.Infof("Failed to upload etcd snapshot file: %v, retried %d times", err, retries+1)
			if retries >= s3ServerRetries {
				return fmt.Errorf("failed to upload etcd snapshot file: %v", err)
			}
			continue
		}
		log.Infof("Successfully uploaded backup file %s to the bucket %s", bc.Name, bc.BucketName)
		break
	}
	return nil
}

// Remove implements storage driver remove interface
func (s StorageProvider) Remove(backupTime time.Time, retentionPeriod time.Duration, bc *types.BackupOpts) error {
	log.WithFields(log.Fields{
		"retention": retentionPeriod,
	}).Infof("Invoking delete backup files in %s", bc.Provider)

	svc := s3.New(s3Session)
	var backupDeleteObjs []*s3.ObjectIdentifier
	cutoffTime := backupTime.Add(retentionPeriod * -1)
	listInput := &s3.ListObjectsInput{
		Bucket: aws.String(bc.BucketName),
	}

	// list bucket objects, max 1000 files
	result, err := svc.ListObjects(listInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Error(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				log.Error(aerr.Error())
			}
		} else {
			log.Error(aerr.Error())
		}
		return err
	}

	re := regexp.MustCompile(".+_etcd$")
	for i := range result.Contents {
		if result.Contents[i] != nil {
			objKey := *result.Contents[i].Key
			if re.MatchString(objKey) {
				backupTime, err := time.Parse(time.RFC3339, strings.Split(objKey, "_")[0])
				if err != nil {
					log.WithFields(log.Fields{
						"name":  objKey,
						"error": err,
					}).Warn("Couldn't parse s3 backup")
				} else if backupTime.Before(cutoffTime) {
					backupDeleteObjs = append(backupDeleteObjs, &s3.ObjectIdentifier{
						Key: aws.String(objKey),
					})
				}
			}
		}
	}

	if len(backupDeleteObjs) == 0 {
		log.Info("No s3 backup files found for deletion")
		return nil
	}

	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(bc.BucketName),
		Delete: &s3.Delete{
			Objects: backupDeleteObjs,
			Quiet:   aws.Bool(false),
		},
	}
	resp, err := svc.DeleteObjects(deleteInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Error("Error detected during deletion: ", aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr
			log.Error("Error detected during deletion: ", err.Error())
		}
	}
	log.Infof("Successfully delete total %d backup files in %s:", len(resp.Deleted), bc.Provider)
	for d := range resp.Deleted {
		log.Info(*resp.Deleted[d].Key)
	}
	return nil
}

// Download implements storage driver download interface
func (s StorageProvider) Download(bc *types.BackupOpts, backupBaseDir string) error {
	log.Infof("Invoking downloading backup files: %s from %s", bc.Name, bc.Provider)
	downloader := s3manager.NewDownloader(s3Session)
	writePath := backupBaseDir + "/" + bc.Name
	fi, err := os.Create(writePath)
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		return fmt.Errorf("failed to create file %q, %v", bc.Name, err)
	}

	// Write the contents of S3 Object to the file
	n, err := downloader.Download(fi, &s3.GetObjectInput{
		Bucket: aws.String(bc.BucketName),
		Key:    aws.String(bc.Name),
	})
	if err != nil {
		log.Errorf("Failed to download etcd snapshot file: %s", bc.Name)
		return err
	}
	log.Infof("File %s downloaded, %d bytes", writePath, n)
	return nil
}

func newSession(bc *types.BackupOpts, useSSL bool) (*session.Session, error) {
	log.Info("Invoking set aws s3 service client")

	var disableForce = false
	if bc.Provider == "oss" {
		// for AliYun OSS it requires to use force path
		disableForce = true
	}

	if bc.Region == "" {
		region, err := getBucketRegion(bc)
		if err != nil {
			return nil, err
		}
		bc.Region = region
	}

	s3Config := &aws.Config{}
	// use IAM role if the s3 accessKey and secretKey is not set
	if len(bc.AccessKey) == 0 && len(bc.SecretKey) == 0 {
		log.Info("invoking set s3 service client use IAM role")
		s3Config = &aws.Config{
			Endpoint:   aws.String(bc.Endpoint),
			Region:     aws.String(bc.Region),
			DisableSSL: aws.Bool(!useSSL),
		}
	} else {
		s3Config = &aws.Config{
			Credentials:      awsCredentials.NewStaticCredentials(bc.AccessKey, bc.SecretKey, ""),
			Endpoint:         aws.String(bc.Endpoint),
			Region:           aws.String(bc.Region),
			DisableSSL:       aws.Bool(!useSSL),
			S3ForcePathStyle: aws.Bool(!disableForce),
		}
	}
	return session.Must(session.NewSession(s3Config)), nil
}

func getBucketRegion(bc *types.BackupOpts) (string, error) {
	log.Info("Invoking get bucket region")
	sess := session.Must(session.NewSession())
	region, err := s3manager.GetBucketRegion(context.Background(), sess, bc.BucketName, "us-east-2")
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			log.Errorf("Unable to find bucket %s's region not found, err: %+v", bc.BucketName, aerr.Message())
		}
		return "", err
	}
	log.Infof("Bucket %s is in region %s", bc.BucketName, region)
	return region, nil
}
