import client from '../api/defaultClient';
import {
    SET_CONNECTION,
    SET_STATUS,
    SET_TOPICS,
    SET_SCHEMA,
    SET_DB_INFO,
    SET_OUTPUT_TABLE,
    SET_QUERY_EXECUTING,
    SET_QUERY_EXECUTED,
    SET_QUERY_TERMINATING,
    SET_QUERY_TERMINATED,
    QUERY_ERROR,
    LOAD_ERROR,
    SET_CODE,
    CLOSE_DESCRIPTION,
    SET_DATABASE,
    SET_KEEP_TABLE,
    STATUS_RUNNING,
    APPEND_MESSAGE,
    SET_DATA,
    DATA_LOADING,
    DATA_ERROR
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

export const getTopics = () => (dispatch, getState) => {
    client
        .get('/command/topics')
        .then(topics => {
            if (getState().core.topics.length < topics.data.length) {
                dispatch(setTopics(topics.data));
                return;
            }
            for (let i = 0; i < topics.data.length; i++) {
                if (getState().core.topics[i].name !== topics.data[i].name) {
                    dispatch(setTopics(topics.data));
                    return;
                }
            }
        })
        .catch(err => {
            console.log(err);
            dispatch({ type: LOAD_ERROR });
        });
};

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

export const getSchema = topic => dispatch => {    
    client
        .get('/command/topic', { params: { topicName: topic } })
        .then(schema => {
            dispatch(setSchema({
                topic: topic,
                data: schema.data
            }));
        })
        .catch(err => {
            console.log(err);
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

export const loadSnapshot = query => async dispatch => {
    dispatch(setDataLoading());
    try {
        let response = await client.get('/data', { params: {query}});
        dispatch(setData(response.data));
    }
    catch(err) {        
        dispatch(setDataError(err.response.data));
    }
}

export const setData = data => {
    return {
        type: SET_DATA,
        payload: data
    };
}

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

export const setStatus = status => {
    return {
        type: SET_STATUS,
        payload: status
    };
};

export const setTopics = topics => {
    return {
        type: SET_TOPICS,
        payload: topics
    };
};

export const setSchema = schema => {
    return {
        type: SET_SCHEMA,
        payload: schema
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

export const closeDescription = () => {
    return {
        type: CLOSE_DESCRIPTION
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

export const appendMessages = messages => {
    return {
        type: APPEND_MESSAGE,
        payload: messages
    };
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