from flask import Flask, jsonify, request, Response, send_file
from bson import ObjectId
from pymongo import MongoClient, DESCENDING
import gridfs
import zipfile
import os
from io import BytesIO
from flask_cors import CORS
import shutil

origin='http://127.0.0.1:3000'
app = Flask(__name__)
client = MongoClient(os.getenv("MONGO_URI"))  # Adjust the URI as needed
db = client['data']
fs = gridfs.GridFS(db)
annotations = db['annotations']
CORS(app)

access_control = {'Access-Control-Allow-Origin': '*'}
@app.route("/ping")
def ping():
    return 'pong'


@app.route('/images/', methods=['GET', 'POST','DELETE'])
def images():
    if request.method == 'GET':
        file_id = request.args.get('file_id')
        file_name = request.args.get('file_name')
        if file_id is None and file_name is None:
            files = fs.find()
            return jsonify({str(file._id):file.filename for file in files}), 200, access_control
        elif file_id is not None and file_name is not None:
            file = fs.find_one({'filename': file_name, '_id': ObjectId(file_id)})
        elif file_id is not None:
            file = fs.get(ObjectId(file_id))
        else:
            file = fs.find_one({'filename': file_name})

        if not file:
            return jsonify({'error': 'File not found'}), 404, access_control

        return Response(file.read(), mimetype='application/octet-stream'), 200, access_control

    elif request.method == 'POST':
        if 'image' not in request.files:
            return jsonify({'error': 'No file provided'}), 404, access_control
        if 'name' not in request.form:
            return jsonify({'error': 'No file name provided'}), 404, access_control
        image = request.files['image']
        name = request.form['name']
        file_id = fs.put(image, filename=name)
        return jsonify({'message': 'File uploaded successfully', 'file_id': str(file_id)}), 201, access_control

    elif request.method == 'DELETE':
        file_id = request.form.get('file_id')
        if not file_id:
            return jsonify({'error': 'Missing file_id','form':request.form}), 400, access_control
        file = fs.get(ObjectId(file_id))
        if not file:
            return jsonify({'error': 'File not found'}), 404, access_control
        try:
            annotations.delete_many({'file_id': ObjectId(file_id)})
            fs.delete(ObjectId(file_id))
            return jsonify({"message": "Image and annotations deleted successfully"}), 200, access_control
        except Exception as e:
            return jsonify({"error": e}), 404, access_control

@app.route('/annotations/', methods=['GET', 'POST','DELETE','PUT'])
def annotation_():
    if request.method == 'GET':
        file_id = request.args.get('file_id')
        annotation_id = request.args.get('annotation_id')
        if file_id is None and annotation_id is None:
            data = {str(file._id):[str(ann.get('_id')) for ann in annotations.find({'files_id': ObjectId(file._id)})] for file in fs.find()}
        elif file_id is not None and annotation_id is not None:
            data = annotations.find_one({'files_id': ObjectId(file_id),'_id':ObjectId(annotation_id)}).get('data')
        elif file_id is not None:
            data = {str(ann.get('_id')):ann.get('data') for ann in annotations.find({'files_id': ObjectId(file_id)}, sort=[('_id', DESCENDING)])}
        else:
            data = annotations.find_one({'_id':ObjectId(annotation_id)}).get('data')
        return jsonify(data), 200, access_control

    elif request.method == 'POST':
        json_data = request.get_json()

        if 'file_id' not in json_data:
            return jsonify({'error': 'No file id provided'}), 404, access_control
        file_id = json_data.get('file_id')
        data = json_data.get('data')
        if data is None:
            data={}
        annotation_data = {
            'files_id': ObjectId(file_id),  # Convert file_id to ObjectId for MongoDB
            'data': data  # The dict with lists of numbers
        }
        id = annotations.insert_one(annotation_data).inserted_id
        return jsonify({'message': 'Annotation added successfully', "id": str(id)}), 201, access_control

    elif request.method == 'DELETE':
        if 'annotation_id' not in request.form:
            return jsonify({'error': 'No annotation id provided'}), 404, access_control
        delete_result = annotations.delete_one({'_id': ObjectId(request.form['annotation_id'])})
        if delete_result.deleted_count > 0:
            return jsonify({"message": "Annotation deleted successfully"}), 200, access_control
        else:
            return jsonify({"error": "Annotation not found"}), 404, access_control
    elif request.method == 'PUT':
        json_data = request.get_json()
        if 'annotation_id' not in json_data:
            return jsonify({'error': 'No annotation id provided'}), 404, access_control
        if 'data' not in json_data:
            return jsonify({'error': 'No data provided'}), 404, access_control
        result = annotations.update_one(
            {'_id': ObjectId(json_data['annotation_id'])},  # Find the document by file_id
            {'$set': {'data': json_data['data']}}  # Update the data field
        )

        if result.matched_count == 0:
            return jsonify({'error': 'Annotation not found'}), 404, access_control

        return jsonify({'message': 'Annotation updated successfully'}), 200, access_control


@app.route('/schema/', methods=['GET'])
def schema():
    files = fs.find()
    result = {}
    counts = {}
    for file in files:
        file_id = str(file._id)
        file_annotations = list(annotations.find({'files_id': ObjectId(file._id)}))
        if file.filename in result:
            counts[file.filename] = counts[file.filename] + 1
            result[file.filename + f" ({counts[file.filename]})"] = {
                "id": file_id,
                "annotations": [str(ann["_id"]) for ann in file_annotations]
            }
        else:
            counts[file.filename] = 0
            result[file.filename] = {
                "id": file_id,
                "annotations": [str(ann["_id"]) for ann in file_annotations]
            }

    return jsonify(result), 200

@app.route('/export_data', methods=['GET'])
def export_data():
    try:
        # Define the directories
        images_dir = 'export/images'
        annotations_dir = 'export/annotations'

        # Ensure the directories are empty before use
        if os.path.exists(images_dir):
            shutil.rmtree(images_dir)
        if os.path.exists(annotations_dir):
            shutil.rmtree(annotations_dir)

        # Create new empty directories
        os.makedirs(images_dir, exist_ok=True)
        os.makedirs(annotations_dir, exist_ok=True)

        filenames = {}
        # Retrieve all images from GridFS
        for grid_file in fs.find():
            file_id = str(grid_file._id)
            print(file_id)
            filename = grid_file.filename
            filenames.update({file_id: filename})
            with open(os.path.join(images_dir, filename), 'wb') as f:
                f.write(grid_file.read())

        # Retrieve all annotations from the annotations collection
        for annotation in annotations.find():
            file_id = annotation.get('files_id')
            print(file_id)
            if filename := filenames.get(str(file_id)):
                data = annotation.get('data')
                annotation_filename = f"{filename.split('.')[0]}.txt"
                with open(os.path.join(annotations_dir, annotation_filename), 'w') as f:
                    f.write(str(data))

        # Create a ZIP file
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add images to ZIP
            for root, dirs, files in os.walk(images_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    zip_file.write(file_path, os.path.relpath(file_path, images_dir))

            # Add annotations to ZIP
            for root, dirs, files in os.walk(annotations_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    zip_file.write(file_path, os.path.relpath(file_path, annotations_dir))

        # Clean up the temporary directories after use
        shutil.rmtree(images_dir)
        shutil.rmtree(annotations_dir)

        # Send the ZIP file as a response
        zip_buffer.seek(0)
        return send_file(zip_buffer, as_attachment=True, download_name='export.zip', mimetype='application/zip')

    except Exception as e:
        return jsonify({'error': str(e)}), 500