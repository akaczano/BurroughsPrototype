import React from 'react';
import { Row } from 'react-bootstrap';

import StatusDisplay from './StatusDisplay';

const tabs = ['Status', 'Console', 'Data'];

class OutputTabs extends React.Component {
    state = { selectedTab: 0 };
    getDisplayedComponent() {
        if (this.state.selectedTab == 0) {
            return <StatusDisplay />;
        }
    }
    render() {
        return (
            <>
                <Row className="tab-group" style={{ backgroundColor: '#f1f1f1', margin: '0px', marginTop: '6px' }}>
                    {tabs.map((tab, i) => {
                        let className = i === this.state.selectedTab ? 'tab-button active' : 'tab-button';
                        return (
                            <button
                                className={className}
                                key={tab}
                                onClick={() => this.setState({ selectedTab: i })}
                            >
                                {tab}
                            </button>
                        );
                    })}
                </Row>
                <Row style={{margin:'0px', padding: '5px', paddingRight: '15px'}}>                    
                    {this.getDisplayedComponent()}
                </Row>
            </>
        );
    }
}

export default OutputTabs;
