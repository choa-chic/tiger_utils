# Synchronizing and downloading TIGER/Line Shapefiles with AWS S3 CLI

Depending on your configuration, you may need to log in to AWS first. This will likely open a browser window for authentication.
```sh
aws login
```

To upload the TIGER/Line shapefiles from your local `tiger_data` directory to the specified S3 bucket, use the following command. This command excludes all files except for `.zip` files and the `README.md` file, and it deletes any files in the S3 bucket that are not present in your local directory.

```sh
aws s3 sync ./tiger_data/ s3://<s3-bucket-name>/census/tiger_line/  --exclude "*" --include "*.zip" --include "README.md" --delete
```

To download the TIGER/Line shapefiles from the specified S3 bucket to your local `tiger_data` directory, use the following command. This command excludes all files except for `.zip` files.
```sh
aws s3 sync s3://<s3-bucket-name>/census/tiger_line/ ./tiger_data/  --exclude "*" --include "*.zip"
```