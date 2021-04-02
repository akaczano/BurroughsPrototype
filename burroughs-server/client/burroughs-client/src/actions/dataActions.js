import client from '../api/defaultClient';
import {
    SET_DATA,
    DATA_LOADING,
    DATA_ERROR,  
    SET_QUERY_TEXT
} from './actionTypes';

export const loadSnapshot = query => async dispatch => {
    dispatch(setDataLoading());
    try {
        let response = await client.get('/data', { params: {query}});
        dispatch(setData(response.data));
    }
    catch(err) {        
        dispatch(setDataError(err.response.data));
    }
};

export const setDataLoading = () => {
    return {
        type: DATA_LOADING
    };
};

export const setDataError = msg => {
    return {
        type: DATA_ERROR,
        payload: msg
    };
};

export const setQueryText = sql => {
    return {
        type: SET_QUERY_TEXT,
        payload: sql
    };
};

export const setData = data => {
    return {
        type: SET_DATA,
        payload: data
    };
};