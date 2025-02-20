const fs = require('fs');
const path = require('path');
const readline = require('readline');

const INPUT_FILE = 'input.txt';
const OUTPUT_FILE = 'output.txt';
const TEMP_DIRECTORY = './tmp';
const CHUNK_SIZE = 400 * 1024 * 1024;

// Создаем папку для временных файлов
if (!fs.existsSync(TEMP_DIRECTORY)) {
    fs.mkdirSync(TEMP_DIRECTORY);
}

// Читаем исходный файл по частям, сортируем каждую из них, записываем во временные файлы
async function splitAndSortChunks() {
    try {
        const readStream = fs.createReadStream(INPUT_FILE, { encoding: 'utf8', highWaterMark: CHUNK_SIZE });
        const readline = readline.createInterface({ input: readStream, crlfDelay: Infinity });

        let currentChunk = [];
        let chunkIndex = 0;

        readStream.on('error', (error) => {
            throw new Error(`Ошибка чтения исходного файла: ${error.message}`);
        });

        for await (const line of readline) {
            currentChunk.push(line);
            if (Buffer.byteLength(currentChunk.join('\n'), 'utf8') >= CHUNK_SIZE) {
                currentChunk.sort((a, b) => a.localeCompare(b));
                const tempFilePath = path.join(TEMP_DIRECTORY, `chunk_${chunkIndex}.txt`);
                try {
                    fs.writeFileSync(tempFilePath, currentChunk.join('\n'));
                } catch (error) {
                    throw new Error(`Ошибка записи временного файла ${tempFilePath}: ${error.message}`);
                }
                currentChunk = [];
                chunkIndex++;
            }
        }

        // Проверяем, что все части обработаны
        if (currentChunk.length > 0) {
            currentChunk.sort((a, b) => a.localeCompare(b));
            const tempFilePath = path.join(TEMP_DIRECTORY, `chunk_${chunkIndex}.txt`);
            try {
                fs.writeFileSync(tempFilePath, currentChunk.join('\n'));
            } catch (error) {
                throw new Error(`Ошибка записи временного файла ${tempFilePath}: ${error.message}`);
            }
        }
    } catch (error) {
        throw new Error(`Ошибка в splitAndSortChunks: ${error.message}`);
    }
}

// Функция для слияния отсортированных частей
async function mergeChunks() {
    try {
        const chunks = fs.readdirSync(TEMP_DIRECTORY).map(file => path.join(TEMP_DIRECTORY, file));
        const writeStream = fs.createWriteStream(OUTPUT_FILE);

        writeStream.on('error', (error) => {
            throw new Error(`Ошибка записи в итоговый файл: ${error.message}`);
        });

        const streams = chunks.map(chunk => {
            const stream = fs.createReadStream(chunk);
            stream.on('error', (error) => {
                throw new Error(`Ошибка чтения временного файла ${chunk}: ${error.message}`);
            });
            return readline.createInterface({ input: stream, crlfDelay: Infinity });
        });

        const iterators = streams.map(stream => stream[Symbol.asyncIterator]());

        const currentLines = await Promise.all(iterators.map(async (iterator) => {
            try {
                const { value } = await iterator.next();
                return value;
            } catch (error) {
                throw new Error(`Ошибка чтения строки из временного файла: ${error.message}`);
            }
        }));

        // Функция поиска индекса минимальной строки среди текущих строк всех чанков
        function findMinIndex() {
            let minIndex = -1;
            let minValue = null;
            for (let i = 0; i < currentLines.length; i++) {
                if (currentLines[i] !== null && (minValue === null || currentLines[i] < minValue)) {
                    minValue = currentLines[i];
                    minIndex = i;
                }
            }
            return minIndex;
        }

        while (true) {
            const minIndex = findMinIndex();
            if (minIndex === -1) break;

            try {
                writeStream.write(currentLines[minIndex] + '\n');
            } catch (error) {
                throw new Error(`Ошибка записи строки в итоговый файл: ${error.message}`);
            }

            try {
                const { value, done } = await iterators[minIndex].next();
                if (done) {
                    currentLines[minIndex] = null;
                } else {
                    currentLines[minIndex] = value;
                }
            } catch (error) {
                throw new Error(`Ошибка чтения строки из временного файла: ${error.message}`);
            }
        }

        streams.forEach(stream => stream.close());
        writeStream.end();

        // Удаляем временные файлы и папку
        chunks.forEach(chunk => {
            try {
                fs.unlinkSync(chunk);
            } catch (error) {
                console.error(`Ошибка удаления временного файла ${chunk}: ${error.message}`);
            }
        });

        try {
            fs.rmdirSync(TEMP_DIRECTORY);
        } catch (error) {
            console.error(`Ошибка удаления временной папки: ${error.message}`);
        }
    } catch (error) {
        throw new Error(`Ошибка в mergeChunks: ${error.message}`);
    }
}

async function main() {
    try {
        await splitAndSortChunks();
        await mergeChunks();
        console.log('Сортировка завершена!');
    } catch (error) {
        console.error('Произошла ошибка:', error.message);
        process.exit(1);
    }
}

main();
