const AWS = require('aws-sdk');
const { ParquetWriter } = require('parquetjs');

const s3 = new AWS.S3();

exports.handler = async (event, context) => {
    try {
        const output = [];
        const records = event.records;

        for (const record of records) {
            const payload = Buffer.from(record.data, 'base64').toString('utf-8');
            console.log('Payload:', payload);

            // Transform the payload data into Parquet format
            const parquetBuffer = await createParquetFile(payload);

            // Upload the Parquet-formatted data to S3
            const params = {
                Bucket: 'prashik123',
                Key: `output-${Date.now()}.parquet`,
                Body: parquetBuffer,
                ContentType: 'application/octet-stream'
            };

            const s3Result = await s3.upload(params).promise();
            console.log('Uploaded to S3:', s3Result);

            output.push({
                recordId: record.recordId,
                result: 'Ok',
                data: {
                    location: s3Result.Location
                }
            });
        }

        console.log(`Processed ${records.length} records.`);
        return { records: output };
    } catch (error) {
        console.error('Error processing records:', error);
        throw error;
    }
};

async function createParquetFile(data) {
    // Define your Parquet schema and create a Parquet writer
    const schema = new ParquetWriter.Schema({
        // Define the schema according to your data structure
    });

    const writer = await ParquetWriter.openFile(schema, 'output.parquet');

    // Append rows to the Parquet writer
    // You'll need to parse your data and append it properly to the Parquet file
    // Example: await writer.appendRow({ columnName: data });

    // Close the Parquet writer and return the buffer
    await writer.close();
    const parquetBuffer = await writer.toBuffer();
    return parquetBuffer;
}
