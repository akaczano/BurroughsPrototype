import client from '../api/defaultClient';
import {
    SET_CONNECTION,
    SET_STATUS,
    SET_TOPICS,
    SET_SCHEMA,
    SET_DB_INFO,
    SET_OUTPUT_TABLE_UPDATED,
    SET_QUERY_EXECUTING,
    SET_QUERY_EXECUTED,
    SET_QUERY_TERMINATING,
    SET_QUERY_TERMINATED,
    LOAD_ERROR,
    SET_CODE,
    CLOSE_DESCRIPTION    
} from './actionTypes';

export const getConnection = () => dispatch => {
    client
        .get('/command/connection')
        .then(connection => {
            dispatch(setConnection(connection));
        })
        .catch(err => {
            dispatch({type: LOAD_ERROR});
        });
}; 

export const getTopics = () => dispatch => {
    client
        .get('/command/topics')
        .then(topics => {
            dispatch(setTopics(topics.data));
        })
        .catch(err => {
            console.log(err);
            dispatch({type: LOAD_ERROR});
        });
};

export const getStatus = () => dispatch => {
    client
        .get('/command/status')
        .then(status => {
            dispatch(setStatus(status));
        })
        .catch(err => {
            dispatch({type: LOAD_ERROR});
        });
};

export const getSchema = topic => dispatch => {
    client        
        .get('/command/topic', { params: {topicName: topic} })
        .then(schema => {
            dispatch(setSchema(schema));
        })
        .catch(err => {
            dispatch({type: LOAD_ERROR});
        });
};

export const setOutputTable = table => dispatch => {
    client
        .post('/command/table', { params: {tableName: table} })
        .then(() => {
            dispatch(setOutputUpdated(table));
        })
        .catch(() => {
            dispatch({type: LOAD_ERROR});
        });
};

export const executeQuery = query => dispatch => {
    dispatch({type: SET_QUERY_EXECUTING});    
    client
        .post('/execute', {query})
        .then(() => {
            dispatch({type: SET_QUERY_EXECUTED});
        })
        .catch(() => {
            dispatch({type: LOAD_ERROR});
        })
}; 

export const terminateQuery = () => dispatch => {
    dispatch({type: SET_QUERY_TERMINATING});
    client
        .post('/command/stop')
        .then(() => {
            dispatch({type: SET_QUERY_TERMINATED});
        })
        .catch(() => {
            dispatch({type: LOAD_ERROR});
        })
}


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

export const setOutputUpdated = table =>  {
    return {
        type: SET_OUTPUT_TABLE_UPDATED,
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