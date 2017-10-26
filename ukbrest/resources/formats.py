import json

from flask import Response

from ukbrest.common.utils.constants import BGEN_SAMPLES_TABLE


class GenericSerializer():
    def data_generator(self, all_data, data_conversion_func, **kwargs):
        from io import StringIO

        for row_idx, row in enumerate(all_data):
            f = StringIO()
            data_conversion_func(row, f, header=(row_idx == 0), **kwargs)

            yield f.getvalue()

    def _get_args(self, *args):
        data = args[0]
        code = args[1]

        return data, code

    def _get_value_from_dict(self, header_name, dictionary, default_value=None):
        return dictionary[header_name] if header_name in dictionary else default_value

    def serialize(self, data_frame, out_buffer, **kwargs):
        raise Exception('Not implemented')

    def get_order_by_table(self):
        return None

    def __call__(self, *args, **kwargs):
        data, code = self._get_args(*args)
        missing_code = self._get_value_from_dict('missing_code', data, default_value='NA')

        headers = self._get_value_from_dict('headers', kwargs, {})

        resp = Response(
            self.data_generator(
                data['data'],
                self.serialize,
                na_rep=missing_code
            ),
            code
        )

        resp.headers.extend(headers or {})
        return resp


class CSVSerializer(GenericSerializer):
    def serialize(self, data_frame, out_buffer, **kwargs):
        data_frame.to_csv(out_buffer, **kwargs)


class BgenieSerializer(GenericSerializer):
    def get_order_by_table(self):
        return BGEN_SAMPLES_TABLE

    def serialize(self, data_frame, out_buffer, **kwargs):
        data_frame.to_csv(out_buffer, sep=' ', index=False, **kwargs)


class Plink2Serializer(GenericSerializer):
    def serialize(self, data_frame, out_buffer, **kwargs):
        data_frame.index.name = 'FID'
        data = data_frame.assign(IID=data_frame.index.values.copy())

        columns = data.columns.tolist()
        columns_reordered = ['IID'] + [c for c in columns if c != 'IID']
        data = data.loc[:, columns_reordered]

        kwargs['na_rep'] = 'NA'

        data.to_csv(out_buffer, sep='\t', **kwargs)


class JsonSerializer(GenericSerializer):
    def __call__(self, *args, **kwargs):
        data, code = self._get_args(*args)
        headers = self._get_value_from_dict('headers', kwargs, {})

        if isinstance(data, dict) and 'data' in data:
            data = data['data']

        resp = Response(json.dumps(data), code)
        resp.headers.extend(headers or {})
        return resp
