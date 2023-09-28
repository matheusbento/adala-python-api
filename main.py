import array
import json
import time
import logging
import os
import sys

from builder.builder import Builder
from flask_cors import CORS, cross_origin
from flask import Flask, jsonify, request, abort, Response, send_file
import numpy as np
import io

from report.report import Report

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route("/process", methods=['GET'])
def process():
    args = request.args
    extra = None
    identifier = args.get("identifier", default=None, type=str)
    if identifier is None:
        return abort(400, dict({'error': 'Identifier is null'}))
    object_paths = args.get("object_paths", default=None, type=str)
    if object_paths is None:
        return abort(400, dict({'error': 'Object Path is null'}))
    attributes = args.get("attributes", default=None, type=str)
    if attributes is None:
        return abort(400, dict({'error': 'Attributes is null'}))
    extraParams = args.get("extra", default=None, type=str)
    if extraParams is not None:
        extra = json.loads(extraParams)
    schema = identifier
    object_paths = json.loads(object_paths)
    attributes = json.loads(attributes)
    start_time = time.time()
    print('Init: Import_Data')
    print(object_paths)
    try:
        builder = Builder(schema, object_paths, attributes, None, extra)
        builder.process()
    except Exception as e:
        raise e
        return abort(400, e)
    end_time = time.time() - start_time
    print('Finish: Import_Data | Duration: %s' % end_time)
    return "FINISH"


@app.route("/data", methods=['GET'])
@cross_origin()
def get_data():
    args = request.args
    data_processing_method = args.get("processing_method", default=None, type=str)
    chart_type = args.get("chart_type", default=None, type=str)
    data_column = args.get("select", default=None, type=str)
    identifier = args.get("identifier", default=None, type=str)
    filter = args.get("filter", default=None, type=str)
    if filter is not None:
        print(filter)
        filter = json.loads(filter)
    if identifier is None:
        return abort(400, dict({'error': 'Identifier is null'}))
    try:
        report = Report(identifier)
        my_tuple = report.get(chart_type, data_processing_method, data_column, filter)

        if data_processing_method is not None:
            if data_processing_method == 'mean':
                mean = []
                for i in range(len(my_tuple)):
                    mean.append(np.mean(my_tuple[i]))
                return jsonify(data=mean)

        return jsonify(data=my_tuple)
    except Exception as e:
        raise e
        return abort(400, e)

@app.route("/data/generate_fits", methods=['GET'])
@cross_origin()
def generate_fits():
    args = request.args
    data_processing_method = args.get("data_processing_method", default=None, type=str)
    chart_type = args.get("chart_type", default=None, type=str)
    data_column = args.get("data_column", default=None, type=str)
    identifier = args.get("identifier", default=None, type=str)
    filter = args.get("filter", default=None, type=str)
    if filter is not None:
        print(filter)
        filter = json.loads(filter)
    if identifier is None:
        return abort(400, dict({'error': 'Identifier is null'}))
    try:
        report = Report(identifier)
        hdu = report.generate(chart_type, data_processing_method, data_column, filter)

        print(hdu, hdu.header);
        # Crie um objeto de memória para armazenar o FITS
        memory_file = io.BytesIO()

        # Escreva os dados FITS no objeto de memória
        hdu.writeto(memory_file)

        # Configure o ponteiro do objeto de memória para o início
        memory_file.seek(0)

        # Retorne o arquivo FITS como uma resposta HTTP
        return Response(memory_file.read(), content_type='application/fits')
    except Exception as e:
        raise e
        return abort(400, e)