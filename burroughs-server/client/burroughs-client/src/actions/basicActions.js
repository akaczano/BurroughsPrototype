import client from '../api/defaultClient';
import {
    SET_CONNECTION,    
    SET_DB_INFO,
    SET_OUTPUT_TABLE,    
    LOAD_ERROR,
    SET_CODE,    
    SET_DATABASE,     
    APPEND_MESSAGE   
} from './actionTypes';

export const getConnection = () => dispatch => {
    client
        .get('/command/connection')
        .then(connection => {
            dispatch(setConnection(connection.data));
        })
        .catch(err => {
            dispatch({ type: LOAD_ERROR });
        });
};

export const loadDatabase = () => async dispatch => {
    try {
        let data = await client.get('/database');
        dispatch(setDatabase(data.data));
    }
    catch (err) {
        dispatch({ type: LOAD_ERROR });
    }
}

export const loadMessages = () => async (dispatch, getState) => {
    if (!getState().core.connection) return;
    try {
        let data = await client.get('/console', {params: {lastQuery: getState().core.lastConsoleRequest}})        
        dispatch(appendMessages(data.data));        
    }
    catch(err) {
        console.log(err);
        dispatch({type: LOAD_ERROR});
    }
}

export const reconnect = () => async dispatch => {
    try {
        await client.post('/command/connect');
        dispatch(getConnection());
        dispatch(loadDatabase());
    }
    catch(err) {
        console.log(err);
        dispatch({type: LOAD_ERROR});
    }
};




export const setDatabase = database => {
    return {
        type: SET_DATABASE,
        payload: database
    };
};


export const setConnection = conn => {
    return {
        type: SET_CONNECTION,
        payload: conn
    };
};



export const setDBInfo = dbInfo => {
    return {
        type: SET_DB_INFO,
        payload: dbInfo
    };
};

export const setOutputTable = table => {
    return {
        type: SET_OUTPUT_TABLE,
        payload: table
    };
};

export const setCode = code => {
    return {
        type: SET_CODE,
        payload: code
    };
};

export const appendMessages = messages => {
    return {
        type: APPEND_MESSAGE,
        payload: messages
    };
};


