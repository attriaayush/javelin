use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use itertools::Itertools;
use walkdir::WalkDir;

use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_http::byte_stream::{ByteStream, Length};

const CHUNK_SIZE: u64 = 1024 * 1024 * 3; // 3mb chunks

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    bucket_name: String,

    #[arg(short, long)]
    dir_path: String,

    #[arg(short, long, default_value_t = 1)]
    threads: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut files = Vec::new();
    let bucket_name = args.bucket_name;
    for file_path in WalkDir::new(args.dir_path) {
        let file_path = file_path.unwrap();
        if file_path.path().is_dir() {
        } else {
            files.push(FileUpload {
                file_path: file_path.path().to_owned(),
                bucket_name: bucket_name.to_owned(),
            });
        }
    }

    let thread_count = args.threads;
    let chunk_len = (files.len() / thread_count) + 1;

    let chunked_items: Vec<Vec<FileUpload>> = files
        .into_iter()
        .chunks(chunk_len)
        .into_iter()
        .map(|chunk| chunk.collect())
        .collect();

    let mut threads = vec![];
    for file_chunk in chunked_items {
        let timeout_config = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(30))
            .build();
        let shared_config = aws_config::from_env()
            .timeout_config(timeout_config)
            .load()
            .await;
        let client = Client::new(&shared_config);

        let handle = tokio::spawn(async move {
            for file in file_chunk {
                println!("Uploading {}", &file.file_path.display());
                upload_multipart(&file, &client).await.unwrap();
            }
        });
        threads.push(handle);
    }

    for thread in threads {
        thread.await.expect("Thread paniced");
    }
}

struct FileUpload {
    file_path: PathBuf,
    bucket_name: String,
}

async fn upload_multipart(file_upload: &FileUpload, client: &Client) -> anyhow::Result<()> {
    let file_path = file_upload.file_path.to_str().unwrap();
    let bucket_name = &file_upload.bucket_name;
    let file_size = tokio::fs::metadata(file_path)
        .await
        .expect("it exists I swear")
        .len();

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket_name)
        .key(file_path)
        .send()
        .await
        .unwrap();

    let upload_id = multipart_upload_res.upload_id().unwrap();

    let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
    let mut size_of_last_chunk = file_size % CHUNK_SIZE;
    if size_of_last_chunk == 0 {
        size_of_last_chunk = CHUNK_SIZE;
        chunk_count -= 1;
    }

    if file_size == 0 {
        return Ok(());
    }

    let mut upload_parts: Vec<CompletedPart> = Vec::new();

    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            CHUNK_SIZE
        };

        let stream = ByteStream::read_from()
            .path(file_path)
            .offset(chunk_index * CHUNK_SIZE)
            .length(Length::Exact(this_chunk))
            .build()
            .await
            .unwrap();

        //Chunk index needs to start at 0, but part numbers start at 1.
        let part_number = (chunk_index as i32) + 1;

        // snippet-start:[rust.example_code.s3.upload_part]
        let upload_part_res = client
            .upload_part()
            .key(file_path)
            .bucket(bucket_name)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket_name)
        .key(file_path)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap();

    Ok(())
}
