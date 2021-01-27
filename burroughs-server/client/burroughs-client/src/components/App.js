import React from 'react';
import { Container, Row, Col } from 'react-bootstrap';
import { Provider } from 'react-redux';

import store from '../store';
import CodeEditor from './CodeEditor';
import QueryButtons from './QueryButtons';
import TopicList from './TopicList';

const colStyle = {
    paddingBottom: '100%',
    marginBottom: '-100%',
    overflow: 'hidden'
}

class App extends React.Component {
    render() {
        return (
            <Provider store={store}>                
                    <Row style={{marginTop: '20px'}}>
                        <Col md={3} style={colStyle}>
                            <TopicList />
                        </Col>
                        <Col md={9} style={colStyle}>
                            <CodeEditor />
                            <QueryButtons />
                        </Col>
                    </Row>                
            </Provider >
        );
    }

}

export default App;