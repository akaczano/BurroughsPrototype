import client from '../api/defaultClient';
import {
    SET_QUERY_EXECUTING,
    SET_QUERY_EXECUTED,
    SET_QUERY_TERMINATING,
    SET_QUERY_TERMINATED,
    QUERY_ERROR,
    SET_KEEP_TABLE,   
    SET_STATUS,
    STATUS_RUNNING,
    LOAD_ERROR
} from './actionTypes';

export const getStatus = () => dispatch => {
    dispatch({ type: STATUS_RUNNING });
    client
        .get('/command/status')
        .then(status => {
            dispatch(setStatus(status.data));
        })
        .catch(err => {
            dispatch({ type: LOAD_ERROR });
        });
};

export const executeQuery = query => async (dispatch, getState) => {
    dispatch({ type: SET_QUERY_EXECUTING });
    try {
        await client.post('/command/table', null, { params: { tableName: getState().core.dbInfo.table } })
        await client.post('/execute', { query });
        dispatch({ type: SET_QUERY_EXECUTED });
    }
    catch (err) {
        dispatch(setQueryError(err.response.data));
    }
};

export const terminateQuery = () => (dispatch, getState) => {
    dispatch({ type: SET_QUERY_TERMINATING });
    client
        .post('/command/stop', null, {params: {keepTable: getState().core.keepTable}})
        .then(() => {
            dispatch({ type: SET_QUERY_TERMINATED });
        })
        .catch(() => {
            dispatch({ type: LOAD_ERROR });
        })
}

export const setStatus = status => {
    return {
        type: SET_STATUS,
        payload: status
    };
};

export const setQueryError = message => {
    return {
        type: QUERY_ERROR,
        payload: message
    };
};

export const setKeepTable = () => {
    return {
        type: SET_KEEP_TABLE
    };
};
