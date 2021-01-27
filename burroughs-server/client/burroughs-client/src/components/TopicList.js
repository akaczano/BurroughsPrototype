import React from 'react';
import { connect } from 'react-redux';
import { getTopics } from '../actions/basicActions';
import { ListGroup } from 'react-bootstrap';

class TopicList extends React.Component {
    componentDidMount() {
        this.props.getTopics();
    }
    render() {
        return (
            <div style={{
                marginLeft: '6px'
            }}>                
                <ListGroup>
                    <ListGroup.Item disabled style={{fontSize: '14px', padding: '1px'}}>
                        Topic List
                    </ListGroup.Item>
                    {this.props.topics.map(topic =>
                        <ListGroup.Item key={topic.name} style={{
                            fontSize: '10px',
                            padding: '1px',
                            overflowX: 'hidden'
                        }}>
                            {topic.name}
                        </ListGroup.Item>
                    )}
                </ListGroup>
            </div>
        );
    }
}

const mapStateToProps = state => {
    return {
        topics: state.core.topics
    };
};

export default connect(mapStateToProps, { getTopics })(TopicList);