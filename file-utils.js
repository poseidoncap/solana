import fs from 'fs/promises';

// Exporting async function to write data to a JSON file with atomic writes, verification, and enhanced logging
export async function writeJsonFile(filePath, data) {
    try {
        // Validate the file path
        if (!filePath || typeof filePath !== 'string') {
            throw new Error('Invalid file path.');
        }

        // Validate the data to be written
        if (data === undefined || data === null) {
            throw new Error('Data is undefined or null.');
        }

        console.log(`Writing data to file: ${filePath}`);

        // Temporary file path for atomic write
        const tempFilePath = `${filePath}.tmp`;

        // Convert data to JSON string with pretty formatting
        const newDataString = JSON.stringify(data, null, 2);

        // Write the new data to the temporary file
        await fs.writeFile(tempFilePath, newDataString, 'utf8');
        console.log(`Data successfully written to temp file: ${tempFilePath}`);

        // Rename the temporary file to the target file (atomic operation)
        await fs.rename(tempFilePath, filePath);
        console.log(`Renamed temp file to ${filePath}, write completed.`);

        // Verify the content after writing by reading the file back
        const writtenData = await fs.readFile(filePath, 'utf8');
        if (writtenData !== newDataString) {
            throw new Error('Written data does not match the expected data.');
        }

        console.log('Data verification successful. File contents match expected data.');

    } catch (error) {
        console.error(`Error writing to file ${filePath}: ${error.message}`);

        // Clean up any leftover temporary file
        try {
            await fs.unlink(`${filePath}.tmp`);
            console.log(`Cleaned up temporary file: ${filePath}.tmp`);
        } catch (unlinkError) {
            console.error(`Error cleaning up temp file: ${unlinkError.message}`);
        }

        throw error;
    }
}

