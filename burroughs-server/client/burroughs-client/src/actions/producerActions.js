import client from '../api/defaultClient';
import {
    PRODUCERS_LOADED,
    PRODUCER_ERROR,    
} from './actionTypes';

export const loadProducers = () => async (dispatch, getState) => {
    if (!getState().core.connection) return;
    try {
        let producers = (await client.get('/producer')).data;
        dispatch(setProducers(producers.map(p => ({...p, limit: -1}))));
    }
    catch (err) {
        dispatch({type: PRODUCER_ERROR})
    }
};

export const executeCommand = (name, command, params) => async dispatch => {
    try {
        await client.post(`/producer/${name}/${command}`, null, {params});
        dispatch(loadProducers());
    }
    catch (err) {
        dispatch({type: PRODUCER_ERROR});
    }
};

export const setProducers = producers => {
    return {
        type: PRODUCERS_LOADED,
        payload: producers
    };
};