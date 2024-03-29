# Keboola Connection Project Restore PHP

PHP library for clone KBC project from backup in:
- Amazon Simple Cloud Storage Service (S3)
- Microsoft Azure Blob Storage

## Usage

Library is available as composer package.

### Installation

```bash
composer require keboola/kbc-project-restore
```

## Development

Clone github repository and build Docker container 

```
git clone https://github.com/keboola/php-storage-api-restore.git
cd php-storage-api-restore
docker-compose build
```

Create `.env` file from this template

```bash
TEST_STORAGE_API_URL=
TEST_STORAGE_API_TOKEN=
TEST_AWS_ACCESS_KEY_ID=
TEST_AWS_SECRET_ACCESS_KEY=
TEST_AWS_REGION=
TEST_AWS_S3_BUCKET=
TEST_AZURE_ACCOUNT_NAME=
TEST_AZURE_ACCOUNT_KEY=
TEST_AZURE_CONTAINER_NAME=
```

- `TEST_STORAGE_API_*` variables are from the destination project
- `TEST_AWS_*` variables are from the S3 bucket where the backup files are stored  _(Use [aws-cf-template.json](./aws-cf-template.json) CloudFormation stack template to create all required AWS resources)_
- `TEST_AZURE_ACCOUNT_` create new Storage Account in your Azure Subscription
- `TEST_AZURE_CONTAINER_NAME` container whhich will be created in your Storage Account


```bash
docker-compose run --rm tests
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
